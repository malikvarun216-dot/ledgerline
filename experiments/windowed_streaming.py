# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Windowed streaming — what a watermark costs: dedup three ways, and the two topics joined
# MAGIC
# MAGIC A **watermark** tells a stateful stream "events older than *(newest event time seen) - delay*
# MAGIC will not come any more": state about them can be thrown away, and a row that still arrives
# MAGIC behind it is **late** — dropped, with no error. It is a bet on how disordered the input is. This
# MAGIC notebook makes the bet on real data and counts what it costs (decisions.md, Session 6 addition 2;
# MAGIC Session 11).
# MAGIC
# MAGIC Both parts replay Bronze's real history the way Bronze wrote it — from version 0, one Bronze commit
# MAGIC per micro-batch (9 batches for `orders`: 8 of ~50,000 messages, then Drill 1's 74 re-sends, which
# MAGIC carry 2016 event times but arrived on 2026-09-28). `Trigger.AvailableNow` only (Free Edition).
# MAGIC Writes scratch tables `workspace.s11.ws_*` and new checkpoints on every run; production untouched.
# MAGIC
# MAGIC - **Part A — drop duplicate order events**, three ways: `dropDuplicatesWithinWatermark` on **event
# MAGIC   time**, on **arrival time**, and an insert-only `MERGE` on `event_id` (Silver's way).
# MAGIC - **Part B — every order line has its stock decrement within 1 hour**: a stream-stream **left outer
# MAGIC   join** with watermarks, tight (1 minute) and loose (1 day), against the batch answer.
# MAGIC
# MAGIC Every expected number is computed from Bronze in batch SQL **before** the streams run.

# COMMAND ----------

# MAGIC %run ../databricks/silver/_merge_inventory_cdc

# COMMAND ----------

import json
import uuid

BRONZE_ORDERS = "workspace.bronze.orders"
# Scratch tables live in their own schema, never in bronze / silver: Unity Catalog allows 100 tables per
# schema, and experiment scratch filled 73 of workspace.silver's (incidents.md, 2026-10-06).
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.s11")
S = "workspace.s11.ws"  # scratch table prefix
RUN = uuid.uuid4().hex[:8]  # new checkpoints every run: a checkpoint is never reused across runs
CP = f"/Volumes/workspace/silver/checkpoints/s11/{RUN}"
COLS = ["event_id", "event_ts", "event_type", "order_id", "_kafka_timestamp", "_batch_id"]
LATE = f"{S}_late_orders"  # a second source, empty until part A2 appends one late event to it

DEDUP_DELAY = "1 hour"
JOIN_WINDOW = "1 hour"  # the business rule: a sale's decrement within 1 hour of the order
JOIN_DELAYS = {"tight": "1 minute", "loose": "1 day"}

DENYLIST = load_denylist()
spark.createDataFrame([(e,) for e in DENYLIST], "event_id string").createOrReplaceTempView("denied_ids")
print(f"run {RUN}; checkpoints {CP}")


def replay(table):
    """Bronze's history in the order Bronze wrote it: from version 0, one commit per micro-batch
    (verified S10: Silver orders' first build saw exactly Bronze's 9 data commits). Needs the files
    Bronze's automatic OPTIMIZE replaced — kept 30 days, until ~2026-10-27."""
    return spark.readStream.option("startingVersion", 0).option("maxFilesPerTrigger", 3).table(table)


def progress(query, label):
    """What Spark reported per micro-batch: the watermark in force, rows held in state, rows the
    stateful operators dropped as late. Collected after the run; evidence is still the tables."""
    rows = []
    for p in query.recentProgress:
        d = json.loads(p.json) if hasattr(p, "json") else dict(p)
        ops = d.get("stateOperators") or []
        rows.append((
            label, int(d["batchId"]), (d.get("eventTime") or {}).get("watermark"),
            sum(int(o.get("numRowsTotal", 0)) for o in ops),
            sum(int(o.get("numRowsDroppedByWatermark", 0)) for o in ops),
        ))
    return rows


PROGRESS_SCHEMA = "stream string, batch_id long, watermark string, state_rows long, dropped_by_watermark long"
all_progress = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## A0 — Bronze `orders` batch by batch, and what each watermark would call late
# MAGIC
# MAGIC Per Bronze batch: rows, distinct events, event-time range, arrival-time range. Then, for each
# MAGIC watermark, the rows that sit **at or behind** *(newest time in all earlier batches) - delay* —
# MAGIC computed here in plain SQL from Bronze, before any stream runs. `first_copies_behind` are the
# MAGIC rows that would be lost for good: the first copy of an event to arrive, not a re-send.

# COMMAND ----------

display(spark.sql(f"""
    SELECT _batch_id, count(*) AS rows, count(DISTINCT event_id) AS events,
           min(event_ts) AS min_event_ts, max(event_ts) AS max_event_ts,
           min(_kafka_timestamp) AS first_arrival, max(_kafka_timestamp) AS last_arrival
    FROM {BRONZE_ORDERS} GROUP BY 1 ORDER BY 1
"""))


def behind(time_col, delay):
    return spark.sql(f"""
        WITH ranked AS (
            SELECT *, row_number() OVER (PARTITION BY event_id
                                         ORDER BY _kafka_timestamp, _kafka_partition, _kafka_offset) AS copy
            FROM {BRONZE_ORDERS}
        ),
        per_batch AS (SELECT _batch_id, max({time_col}) AS newest FROM ranked GROUP BY 1),
        wm AS (
            SELECT _batch_id,
                   max(newest) OVER (ORDER BY _batch_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                     - INTERVAL {delay} AS watermark
            FROM per_batch
        )
        SELECT count_if(r.{time_col} <= wm.watermark)                AS rows_behind,
               count_if(r.{time_col} <= wm.watermark AND r.copy = 1) AS first_copies_behind,
               count_if(r.copy > 1)                                  AS later_copies,
               -- order lines (one SKU of one order) carried by first copies behind the watermark
               coalesce(sum(CASE WHEN r.{time_col} <= wm.watermark AND r.copy = 1
                                  AND r.event_type = 'created' AND r.items IS NOT NULL
                             THEN size(array_distinct(transform(r.items,
                                       i -> concat_ws('|', i.product_id, i.seller_id)))) END), 0)
                                                                     AS lines_behind
        FROM ranked r JOIN wm USING (_batch_id)
    """).first()


bronze_orders = spark.table(BRONZE_ORDERS).agg(
    F.count(F.lit(1)).alias("rows"), F.count_distinct("event_id").alias("events")
).first()
by_event = behind("event_ts", DEDUP_DELAY)
by_arrival = behind("_kafka_timestamp", DEDUP_DELAY)
print(f"Bronze orders: {bronze_orders.asDict()}")
print(f"behind an event-time watermark ({DEDUP_DELAY}):   {by_event.asDict()}")
print(f"behind an arrival-time watermark ({DEDUP_DELAY}): {by_arrival.asDict()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A1 — the same replay, deduplicated three ways
# MAGIC
# MAGIC - **event** — `withWatermark("event_ts", "1 hour").dropDuplicatesWithinWatermark(["event_id"])`:
# MAGIC   remembers each `event_id` until the watermark passes its event time + 1 hour.
# MAGIC - **arrival** — the same on `_kafka_timestamp`, when the message reached Kafka.
# MAGIC - **merge** — `foreachBatch`: one copy per `event_id` in the batch, then `MERGE … WHEN NOT MATCHED
# MAGIC   THEN INSERT`. No watermark: the target table *is* the memory, and it never forgets.

# COMMAND ----------

for name in ("event", "arrival", "merge"):
    spark.sql(f"DROP TABLE IF EXISTS {S}_dedup_{name}")
spark.sql(f"CREATE OR REPLACE TABLE {LATE} AS SELECT {', '.join(COLS)} FROM {BRONZE_ORDERS} WHERE false")
spark.sql(f"CREATE TABLE {S}_dedup_merge AS SELECT {', '.join(COLS)} FROM {BRONZE_ORDERS} WHERE false")


def source():
    return replay(BRONZE_ORDERS).select(*COLS).unionByName(spark.readStream.table(LATE).select(*COLS))


def dedup_by_watermark(name, time_col):
    q = (
        source().withWatermark(time_col, DEDUP_DELAY).dropDuplicatesWithinWatermark(["event_id"])
        .writeStream.option("checkpointLocation", f"{CP}/dedup_{name}")
        .trigger(availableNow=True).toTable(f"{S}_dedup_{name}")
    )
    q.awaitTermination()
    return progress(q, f"dedup_{name}")


def merge_batch(batch_df, batch_id):
    batch_df.dropDuplicates(["event_id"]).createOrReplaceTempView("s11_batch")
    batch_df.sparkSession.sql(f"""
        MERGE INTO {S}_dedup_merge t USING s11_batch b ON t.event_id = b.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)


def dedup_by_merge():
    q = (
        source().writeStream.option("checkpointLocation", f"{CP}/dedup_merge")
        .foreachBatch(merge_batch).trigger(availableNow=True).start()
    )
    q.awaitTermination()
    return progress(q, "dedup_merge")


def dedup_results():
    bronze_events = spark.table(BRONZE_ORDERS).select("event_id").distinct()
    rows = []
    for name in ("event", "arrival", "merge"):
        t = spark.table(f"{S}_dedup_{name}")
        got = t.agg(F.count(F.lit(1)).alias("rows"), F.count_distinct("event_id").alias("events")).first()
        lost = bronze_events.join(t.select("event_id"), "event_id", "left_anti").count()
        late_kept = t.where(F.col("_batch_id") == -1).count()
        rows.append((name, got.rows, got.events, got.rows - got.events, lost, late_kept))
    return spark.createDataFrame(
        rows, "method string, rows long, events long, duplicates_kept long, bronze_events_lost long, "
              "late_event_kept long",
    )


all_progress += dedup_by_watermark("event", "event_ts")
all_progress += dedup_by_watermark("arrival", "_kafka_timestamp")
all_progress += dedup_by_merge()
display(dedup_results())

# COMMAND ----------

# MAGIC %md
# MAGIC ## A2 — one genuinely new event arrives late
# MAGIC
# MAGIC A new event (its own `event_id`) for the earliest order, carrying that order's 2016 event time and
# MAGIC arriving now — a correction sent days after the fact. Appended to the second source; the same three
# MAGIC streams run again from their checkpoints and read only it.

# COMMAND ----------

(
    spark.table(BRONZE_ORDERS).where("event_type = 'created'").orderBy("event_ts", "event_id").limit(1)
    .select(
        F.concat(F.lit(f"s11-late-{RUN}-"), F.col("event_id")).alias("event_id"), "event_ts",
        "event_type", "order_id", F.current_timestamp().alias("_kafka_timestamp"),
        F.lit(-1).cast("long").alias("_batch_id"),
    )
    .write.mode("append").saveAsTable(LATE)
)
all_progress += dedup_by_watermark("event", "event_ts")
all_progress += dedup_by_watermark("arrival", "_kafka_timestamp")
all_progress += dedup_by_merge()
display(dedup_results())

# COMMAND ----------

# MAGIC %md
# MAGIC ## B0 — the batch answer: every order line, and the decrement that matches it
# MAGIC
# MAGIC An **order line** is one SKU in one order (from the `created` event's items); its **decrement** is
# MAGIC the CDC sale event for that SKU, stamped with the order's own time. One sale is removed on purpose
# MAGIC (`LOST_SALE`) so the join has exactly one line it must report as unmatched.

# COMMAND ----------

LOST_SALE = spark.sql(f"""
    SELECT event_id FROM {BRONZE_CDC}
    WHERE op = 'U' AND prev_stock_qty > stock_qty AND event_id NOT IN (SELECT event_id FROM denied_ids)
    ORDER BY event_ts, event_id LIMIT 1
""").first().event_id
spark.createDataFrame([(LOST_SALE,)], "event_id string").createOrReplaceTempView("lost_sale")

# Bronze is read-only here, so this query gives the same rows every time it runs.
lines_batch = spark.sql(f"""
    SELECT DISTINCT order_id, event_ts, sku_key FROM (
        SELECT order_id, event_ts,
               explode(array_distinct(transform(items,
                       i -> concat_ws('|', i.product_id, i.seller_id)))) AS sku_key
        FROM {BRONZE_ORDERS} WHERE event_type = 'created' AND items IS NOT NULL)
""")
lines_batch.createOrReplaceTempView("lines_batch")
answer = spark.sql(f"""
    WITH sales AS (
        SELECT DISTINCT event_id, sku_key, event_ts FROM {BRONZE_CDC}
        WHERE op = 'U' AND prev_stock_qty > stock_qty
          AND event_id NOT IN (SELECT event_id FROM denied_ids)
          AND event_id NOT IN (SELECT event_id FROM lost_sale)
    ),
    matched AS (
        SELECT DISTINCT l.order_id, l.sku_key FROM lines_batch l JOIN sales s
          ON s.sku_key = l.sku_key AND s.event_ts BETWEEN l.event_ts AND l.event_ts + INTERVAL {JOIN_WINDOW}
    )
    SELECT (SELECT count(*) FROM lines_batch) AS lines, (SELECT count(*) FROM matched) AS matched
""").first()
print(f"lost sale (removed on purpose): {LOST_SALE}")
print(f"batch answer: {answer.asDict()}  -> unmatched {answer.lines - answer.matched}")
for label, delay in JOIN_DELAYS.items():
    print(f"{label} ({delay}): order lines behind an event-time watermark, from Bronze: "
          f"{behind('event_ts', delay).lines_behind:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B1 — the stream-stream join, tight and loose
# MAGIC
# MAGIC Order lines (left) `LEFT OUTER JOIN` sales (right) on SKU, sale time within 1 hour after the order.
# MAGIC Both sides carry a watermark; the sales side also drops its re-sent copies with
# MAGIC `dropDuplicatesWithinWatermark`. A matched line is written at once; an unmatched one only when the
# MAGIC watermark has passed *order time + 1 hour* — the moment a match is no longer possible.

# COMMAND ----------


def join_run(label, delay):
    lines = (
        replay(BRONZE_ORDERS).where("event_type = 'created' AND items IS NOT NULL")
        .select("order_id", "event_ts", F.explode(F.array_distinct(F.transform(
            "items", lambda i: F.concat_ws("|", i["product_id"], i["seller_id"])))).alias("sku_key"))
        .withWatermark("event_ts", delay)
    )
    sales = (
        replay(BRONZE_CDC).where("op = 'U' AND prev_stock_qty > stock_qty")
        .where(~F.col("event_id").isin([*DENYLIST, LOST_SALE]))
        .select(F.col("event_id").alias("sale_id"), F.col("sku_key").alias("sale_sku"),
                F.col("event_ts").alias("sale_ts"))
        .withWatermark("sale_ts", delay)
        .dropDuplicatesWithinWatermark(["sale_id"])
    )
    joined = lines.join(
        sales,
        F.expr(f"sku_key = sale_sku AND sale_ts >= event_ts "
               f"AND sale_ts <= event_ts + INTERVAL {JOIN_WINDOW}"),
        "leftOuter",
    )
    out = f"{S}_join_{label}"
    spark.sql(f"DROP TABLE IF EXISTS {out}")
    q = (
        joined.writeStream.option("checkpointLocation", f"{CP}/join_{label}")
        .trigger(availableNow=True).toTable(out)
    )
    q.awaitTermination()
    return progress(q, f"join_{label}")


def join_results(label):
    t = spark.table(f"{S}_join_{label}")
    line = F.struct("order_id", "sku_key")
    got = t.agg(
        F.count_distinct(F.when(F.col("sale_id").isNotNull(), line)).alias("matched"),
        F.count_distinct(F.when(F.col("sale_id").isNull(), line)).alias("unmatched"),
    ).first()
    never_written = lines_batch.join(t.select("order_id", "sku_key").distinct(), ["order_id", "sku_key"],
                                     "left_anti").count()
    return (label, JOIN_DELAYS[label], answer.lines, got.matched, got.unmatched, never_written)


join_rows = []
for label, delay in JOIN_DELAYS.items():
    all_progress += join_run(label, delay)
    join_rows.append(join_results(label))
display(spark.createDataFrame(
    join_rows, "run string, delay string, lines long, matched long, unmatched long, never_written long"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What Spark reported, per micro-batch
# MAGIC
# MAGIC `state_rows`: rows held in state after the batch (the memory a watermark keeps small);
# MAGIC `dropped_by_watermark`: rows a stateful operator threw away as late. Compare with the tables above —
# MAGIC on serverless some progress numbers are known to be wrong (`numInputRows` is 0 under
# MAGIC `foreachBatch`), so the tables are the evidence and this is the explanation.

# COMMAND ----------

display(spark.createDataFrame(all_progress, PROGRESS_SCHEMA).orderBy("stream", "batch_id"))