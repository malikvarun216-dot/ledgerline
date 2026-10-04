# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bronze → Silver for inventory CDC — shared code
# MAGIC
# MAGIC Loaded with `%run ./_merge_inventory_cdc` by `merge_inventory_cdc` (production) and, from Session 10,
# MAGIC by `exp_04` (scratch tables). **Defines functions only — running it writes nothing.**
# MAGIC
# MAGIC Bronze holds every change message exactly as the topic delivered it. Silver keeps two tables:
# MAGIC - `silver.inventory_events` — every change **once**: the 53 denylisted events removed, re-sends
# MAGIC   collapsed;
# MAGIC - `silver.inventory` — the **current stock** per SKU (one product sold by one seller).
# MAGIC
# MAGIC Each micro-batch of new Bronze rows goes through one function, in this order: drop the denylist →
# MAGIC one copy per `event_id` → refuse the batch if its order cannot be decided → add new events to the
# MAGIC log → apply the newest event per SKU to the stock → count chain breaks → one line in the CDC log.
# MAGIC "The newest event per SKU" is read from the whole log, not the batch (since Session 10, exp_04 C): the
# MAGIC log remembers every delete, so an update arriving after a newer delete cannot bring the SKU back.
# MAGIC
# MAGIC The stock `MERGE` is the CDC path, the opposite of the dims. **Absence means nothing here**: a batch
# MAGIC holds only what changed, so there is no `WHEN NOT MATCHED BY SOURCE` — it would delete every SKU that
# MAGIC did not move. Deletes arrive *in* the data, as `op = 'D'`:
# MAGIC
# MAGIC | newest event in the batch for a SKU | in Silver? | clause | result |
# MAGIC |---|---|---|---|
# MAGIC | `D`, higher `seq` than Silver's | yes | `WHEN MATCHED AND s.op = 'D' AND s.seq > t.seq` | delete |
# MAGIC | `I` / `U`, higher `seq` | yes | `WHEN MATCHED AND s.op IN ('I','U') AND s.seq > t.seq` | update |
# MAGIC | anything, not higher | yes | — | nothing: an old or repeated event never overwrites |
# MAGIC | `I` / `U` | no | `WHEN NOT MATCHED AND s.op IN ('I','U')` | insert |
# MAGIC | `D` | no | — | nothing to delete |
# MAGIC
# MAGIC Every write here is safe to repeat, so no `txnVersion` is needed on the MERGEs (decisions.md,
# MAGIC Session 9). The log line is a plain append, so it gets Bronze's `txnAppId` / `txnVersion`.

# COMMAND ----------

import json
import os

from pyspark.sql import Window
from pyspark.sql import functions as F

BRONZE_CDC = "workspace.bronze.inventory_cdc"
SILVER = "workspace.silver"
EVENTS = f"{SILVER}.inventory_events"
STOCK = f"{SILVER}.inventory"
CDC_LOG = f"{SILVER}.inventory_cdc_log"
CHECKPOINTS = "/Volumes/workspace/silver/checkpoints"
DENYLIST_FILE = "2026-09-26_inventory_cdc_denylist.json"
OPS = ("I", "U", "D")

# Same as the dims (Session 8): the change feed only records from the moment it is on, so it is set at
# creation; old files kept 30 days, so time travel and the feed both reach back that far.
TABLE_PROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
}

# What Silver keeps from each Bronze row. `change_reason` is left behind on purpose: nothing in the
# pipeline may read it (decisions.md, Session 1), and a column that is not here cannot be read.
EVENT_COLUMNS = [
    "event_id", "event_ts", "produced_at", "op", "seq",
    "sku_key", "product_id", "seller_id", "stock_qty", "prev_stock_qty",
]
COORDINATES = ["_kafka_partition", "_kafka_offset"]  # where the kept copy sits on the topic
STOCK_COLUMNS = ["sku_key", "product_id", "seller_id", "stock_qty", "seq"]

# Enforced by Delta on every write (Session 10): stock below zero means the feed is broken upstream, and
# the batch should stop rather than land it. The feed's lowest stock_qty is 11.
STOCK_CONSTRAINTS = {"stock_not_negative": "stock_qty >= 0"}


class BatchRefused(Exception):
    """A batch whose order cannot be decided. Nothing was written; the next run retries the same batch."""


def load_denylist(name=DENYLIST_FILE):
    """The event ids a person decided are wrong (ops/incidents/). Found from the notebook's own folder
    upwards, so production (databricks/silver) and experiments (experiments/) read the same file."""
    here = os.getcwd()
    for _ in range(4):
        path = os.path.join(here, "ops", "incidents", name)
        if os.path.exists(path):
            with open(path) as fh:
                return json.load(fh)["event_ids"]
        here = os.path.dirname(here)
    raise FileNotFoundError(f"{name} not found above {os.getcwd()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables

# COMMAND ----------


def ensure_properties(table):
    """For a table created before a property was chosen. Checked first: every ALTER is a commit."""
    have = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    for name, value in TABLE_PROPERTIES.items():
        if have.get(name) != value:
            spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('{name}' = '{value}')")
            print(f"{table}: set {name} = {value}")


def ensure_constraints(table, constraints):
    """Add each CHECK constraint the table lacks (same as `_merge_orders.ensure_constraints`). Delta checks
    every existing row first; a row that breaks it fails the ALTER and nothing changes."""
    have = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}

    def canonical(rule):  # Delta may store the rule with other spacing, case, brackets or backticks
        return "".join(ch for ch in rule.lower() if ch not in " ()`")

    for name, rule in constraints.items():
        stored = have.get(f"delta.constraints.{name}")
        if stored is None:
            spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({rule})")
            print(f"{table}: added constraint {name}")
        elif canonical(stored) != canonical(rule):
            raise ValueError(f"{table}: constraint {name} is {stored!r}, the code says {rule!r}")


def create_tables(events=EVENTS, stock=STOCK, log=CDC_LOG):
    """In the notebook's own session, before the stream starts: inside foreachBatch the session is a
    clone, where a catalog lookup once answered wrongly (incidents.md, 2026-09-26)."""
    props = ", ".join(f"'{k}' = '{v}'" for k, v in TABLE_PROPERTIES.items())
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {events} (
            event_id STRING NOT NULL, event_ts TIMESTAMP, produced_at TIMESTAMP, op STRING, seq BIGINT,
            sku_key STRING, product_id STRING, seller_id STRING, stock_qty INT, prev_stock_qty INT,
            _kafka_partition INT, _kafka_offset BIGINT, _merged_at TIMESTAMP
        ) TBLPROPERTIES ({props})"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {stock} (
            sku_key STRING NOT NULL, product_id STRING, seller_id STRING, stock_qty INT, seq BIGINT,
            _event_id STRING, _event_ts TIMESTAMP, _kafka_partition INT, _kafka_offset BIGINT,
            _merged_at TIMESTAMP
        ) TBLPROPERTIES ({props})"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {log} (
            batch_id BIGINT, bronze_rows BIGINT, denied_rows BIGINT, duplicate_copies BIGINT,
            events BIGINT, skus BIGINT, events_inserted BIGINT, newest_is_delete BIGINT,
            inserted BIGINT, updated BIGINT, deleted BIGINT, chain_breaks BIGINT,
            events_version BIGINT, stock_version BIGINT, applied_at TIMESTAMP
        )"""
    )
    for table in (events, stock):
        ensure_properties(table)
    ensure_constraints(stock, STOCK_CONSTRAINTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## One micro-batch, step by step
# MAGIC
# MAGIC Every function below takes its Spark session from the DataFrame it is given, never the notebook's
# MAGIC `spark`: inside `foreachBatch` they run in the batch's own session.

# COMMAND ----------


def clean_events(batch, denylist):
    """Bronze rows -> (flagged, events).

    `flagged`: every Bronze row with `_denied`, for counting. `events`: denylisted events removed and
    one copy per `event_id` kept — the lowest Kafka offset, so the same copy wins on every replay.
    """
    denied = F.col("event_id").isin(list(denylist)) if denylist else F.lit(False)
    flagged = batch.select(*EVENT_COLUMNS, *COORDINATES, denied.alias("_denied"))
    first_copy = Window.partitionBy("event_id").orderBy(*COORDINATES)
    events = (
        flagged.where(~F.col("_denied"))
        .withColumn("_copy", F.row_number().over(first_copy))
        .where("_copy = 1")
        .drop("_copy", "_denied")
    )
    return flagged, events


def find_problems(events, events_table):
    """Every way this batch's order cannot be decided. Must be empty. Reads only.

    - `tied_seq`: two different events with one (sku_key, seq) inside the batch;
    - `tied_seq_vs_log`: an event whose (sku_key, seq) is already in the log under another event_id;
    - `bad_op`: an op that is not I, U or D (it would match no clause, or be inserted as stock).
    `seq` is the order. Two events in one place of it cannot be put in order by any rule: keeping the
    first is the Session 5 incident, keeping the latest offset picks the contamination (decisions.md).
    """
    s = events.sparkSession
    logged = s.table(events_table)
    in_batch = events.groupBy("sku_key", "seq").count().where(F.col("count") > 1).count()
    vs_log = (
        events.alias("e")
        .join(
            logged.alias("l"),
            (F.col("e.sku_key") == F.col("l.sku_key"))
            & (F.col("e.seq") == F.col("l.seq"))
            & (F.col("e.event_id") != F.col("l.event_id")),
        )
        .select("e.sku_key", "e.seq")
        .distinct()
        .count()
    )
    bad_op = events.where(F.col("op").isNull() | ~F.col("op").isin(*OPS)).count()
    found = {"tied_seq": in_batch, "tied_seq_vs_log": vs_log, "bad_op": bad_op}
    return {k: v for k, v in found.items() if v}


def latest_per_sku(events):
    """In-batch dedup: the highest-seq event per SKU. Without it, a SKU changed twice in one batch is two
    source rows for one target row, and Delta refuses the MERGE ("multiple source rows matched")."""
    newest = Window.partitionBy("sku_key").orderBy(F.col("seq").desc())
    return events.withColumn("_rank", F.row_number().over(newest)).where("_rank = 1").drop("_rank")


def newest_in_log(events, events_table):
    """The newest event per SKU over the whole event log, for the SKUs this batch touched.

    The log already holds this batch (it is written first) and every earlier event, deletes included. So
    an update that arrives late, after a newer delete, is not the newest event of its SKU — the delete is
    — and the stock MERGE never sees it. With `latest_per_sku(events)` alone it would be the newest in its
    batch, match no row (the delete removed it) and be inserted: the SKU comes back (exp_04 C).
    """
    s = events.sparkSession
    touched = events.select("sku_key").distinct()
    logged = s.table(events_table).join(touched, "sku_key").select(*EVENT_COLUMNS, *COORDINATES)
    return latest_per_sku(logged)


def events_merge_sql(target, source):
    """Insert-only on event_id: an event already logged is left alone, so a replay inserts nothing."""
    cols = [*EVENT_COLUMNS, *COORDINATES]
    return (
        f"MERGE INTO {target} AS t\n"
        f"USING {source} AS s\n"
        f"ON t.event_id = s.event_id\n"
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}, _merged_at) "
        f"VALUES ({', '.join('s.' + c for c in cols)}, current_timestamp())"
    )


def stock_merge_sql(target, source, *, seq_guard=True):
    """The CDC MERGE. `seq_guard=False` exists for exp_04 (Session 10); production never passes it.

    The guard is on the delete too: a D older than the row it meets is history, not the latest word.
    """
    newer = " AND s.seq > t.seq" if seq_guard else ""
    sets = [f"t.{c} = s.{c}" for c in STOCK_COLUMNS if c != "sku_key"] + [
        "t._event_id = s.event_id",
        "t._event_ts = s.event_ts",
        "t._kafka_partition = s._kafka_partition",
        "t._kafka_offset = s._kafka_offset",
        "t._merged_at = current_timestamp()",
    ]
    cols = [*STOCK_COLUMNS, "_event_id", "_event_ts", "_kafka_partition", "_kafka_offset", "_merged_at"]
    vals = [f"s.{c}" for c in STOCK_COLUMNS] + [
        "s.event_id", "s.event_ts", "s._kafka_partition", "s._kafka_offset", "current_timestamp()",
    ]
    return (
        f"MERGE INTO {target} AS t\n"
        f"USING {source} AS s\n"
        f"ON t.sku_key = s.sku_key\n"
        f"WHEN MATCHED AND s.op = 'D'{newer} THEN DELETE\n"
        f"WHEN MATCHED AND s.op IN ('I', 'U'){newer} THEN UPDATE SET {', '.join(sets)}\n"
        f"WHEN NOT MATCHED AND s.op IN ('I', 'U') THEN INSERT ({', '.join(cols)}) VALUES ({', '.join(vals)})"
    )


def chain_breaks(events, events_table):
    """How many of this batch's events start somewhere other than where the SKU's previous event ended:
    `prev_stock_qty` differs from the `stock_qty` before it, by seq, in the log (this batch included).

    The one check that sees order. The feed has 1,423 such links on 237 SKUs, a source defect counted
    rather than repaired (incidents.md 2026-10-01). A rise means a new one.
    """
    s = events.sparkSession
    by_seq = Window.partitionBy("sku_key").orderBy("seq")
    history = (
        s.table(events_table)
        .join(events.select("sku_key").distinct(), "sku_key")
        .withColumn("_before", F.lag("stock_qty").over(by_seq))
    )
    return (
        history.join(events.select("event_id"), "event_id")
        .where(F.col("_before").isNotNull() & ~F.col("prev_stock_qty").eqNullSafe(F.col("_before")))
        .count()
    )


def _version(s, table):
    return s.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first().version


def _merge(s, table, sql):
    """Run one MERGE; what it did, read from the table's own history — the MERGE commit after the
    version we started from, never "the latest version" (Delta adds OPTIMIZE commits of its own)."""
    before = _version(s, table)
    s.sql(sql)
    m = (
        s.sql(f"DESCRIBE HISTORY {table}")
        .where(f"version > {before} AND operation = 'MERGE'")
        .orderBy("version")
        .first()
    )
    if m is None:  # never seen: a MERGE that changes nothing still commits (exp_03 C)
        return {"inserted": 0, "updated": 0, "deleted": 0, "version": None}
    metrics = m.operationMetrics
    return {
        "inserted": int(metrics.get("numTargetRowsInserted", 0)),
        "updated": int(metrics.get("numTargetRowsUpdated", 0)),
        "deleted": int(metrics.get("numTargetRowsDeleted", 0)),
        "version": m.version,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## The batch function and the stream

# COMMAND ----------


def make_batch_writer(
    *,
    denylist,
    app_id,
    events_table=EVENTS,
    stock_table=STOCK,
    log_table=CDC_LOG,
    dedup=True,
    seq_guard=True,
    refuse=True,
    newest_from="log",
):
    """Return the foreachBatch function.

    `dedup`, `seq_guard` and `refuse` exist ONLY for exp_04 (Session 10), which removes each guard to
    watch it break. Production never passes them.

    `newest_from`: where the stock MERGE takes each SKU's newest event from — `"log"` (production since
    Session 10: the whole event log, so a late update cannot resurrect a deleted SKU; `newest_in_log`) or
    `"batch"` (Session 9's behaviour, kept so exp_04 C can show the bug).

    Nothing printed in here reaches the notebook on serverless (incidents.md, Drill 1): the evidence is
    the CDC log table and the tables' history.
    """
    denylist = list(denylist)
    if newest_from not in ("batch", "log"):
        raise ValueError(f"newest_from must be 'batch' or 'log', not {newest_from!r}")

    def apply_batch(batch_df, batch_id):
        s = batch_df.sparkSession
        flagged, events = clean_events(batch_df, denylist)

        problems = find_problems(events, events_table)
        if problems and refuse:
            raise BatchRefused(
                f"batch {batch_id}: {problems}. Nothing was written. Two different events claim one place "
                "in a SKU's order (or an op is not I/U/D); a person decides which is wrong and adds it to "
                "the denylist. The next run retries this batch."
            )

        seen = flagged.agg(
            F.count(F.lit(1)).alias("rows"), F.sum(F.col("_denied").cast("int")).alias("denied")
        ).first()
        size = events.agg(
            F.count(F.lit(1)).alias("events"), F.count_distinct("sku_key").alias("skus")
        ).first()

        events.createOrReplaceTempView("_cdc_events")
        logged = _merge(s, events_table, events_merge_sql(events_table, "_cdc_events"))

        # The WHOLE batch, not only the events new to the log: if a run died between the two MERGEs, the
        # replay finds every event already logged, and "new only" would never apply them to the stock.
        if not dedup:
            newest = events
        elif newest_from == "log":
            newest = newest_in_log(events, events_table)
        else:
            newest = latest_per_sku(events)
        newest.createOrReplaceTempView("_cdc_newest")
        newest_deletes = newest.where("op = 'D'").count()
        applied = _merge(s, stock_table, stock_merge_sql(stock_table, "_cdc_newest", seq_guard=seq_guard))

        breaks = chain_breaks(events, events_table)

        def n(value, name):
            return F.lit(value).cast("bigint").alias(name)

        line = s.range(1).select(
            n(batch_id, "batch_id"),
            n(seen.rows, "bronze_rows"),
            n(seen.denied or 0, "denied_rows"),
            n(seen.rows - (seen.denied or 0) - size.events, "duplicate_copies"),
            n(size.events, "events"),
            n(size.skus, "skus"),
            n(logged["inserted"], "events_inserted"),
            n(newest_deletes, "newest_is_delete"),
            n(applied["inserted"], "inserted"),
            n(applied["updated"], "updated"),
            n(applied["deleted"], "deleted"),
            n(breaks, "chain_breaks"),
            n(logged["version"], "events_version"),
            n(applied["version"], "stock_version"),
            F.current_timestamp().alias("applied_at"),
        )
        # An append is NOT safe to repeat, so the Bronze treatment: Delta remembers (app id, last
        # batch) in this table's log and skips a replayed batch's line.
        (
            line.write.format("delta").mode("append")
            .option("txnAppId", app_id).option("txnVersion", batch_id)
            .saveAsTable(log_table)
        )

    return apply_batch


def run_silver_cdc(
    *,
    checkpoint,
    app_id,
    denylist,
    source=BRONZE_CDC,
    events_table=EVENTS,
    stock_table=STOCK,
    log_table=CDC_LOG,
    **switches,
):
    """One AvailableNow pass: every Bronze row not yet seen, in micro-batches, then stop.

    A deleted checkpoint is harmless to the data here — every MERGE is safe to repeat — but with the
    same app id the CDC log would silently skip its lines for batch ids already used. Same rule as
    Bronze: a reset is a new GENERATION (checkpoint and app id together).
    """
    create_tables(events_table, stock_table, log_table)
    query = (
        spark.readStream.table(source)
        .writeStream.foreachBatch(
            make_batch_writer(
                denylist=denylist,
                app_id=app_id,
                events_table=events_table,
                stock_table=stock_table,
                log_table=log_table,
                **switches,
            )
        )
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks shared by production and exp_04 — each one a different code path from the stream

# COMMAND ----------


def expected_stock(events_table=EVENTS):
    """The stock Silver must hold, recomputed in one batch query over the log: each SKU's highest-seq
    event, unless it is a delete. No MERGE, no micro-batches."""
    newest = Window.partitionBy("sku_key").orderBy(F.col("seq").desc())
    return (
        spark.table(events_table)
        .withColumn("_rank", F.row_number().over(newest))
        .where("_rank = 1 AND op <> 'D'")
        .select(*STOCK_COLUMNS)
    )


def compare_stock(stock_table, expected):
    """(rows in Silver the log does not explain, rows the log expects that Silver lacks). (0, 0) = equal."""
    silver = spark.table(stock_table).select(*STOCK_COLUMNS)
    return silver.exceptAll(expected).count(), expected.exceptAll(silver).count()


def chain_report(events_table=EVENTS):
    """Over the whole log: broken links, SKUs with one, and live SKUs whose newest after-image differs
    from seed + sum of deltas (count, units). Also units sold — the Session 6 tie-out."""
    return spark.sql(
        f"""
        WITH ordered AS (
          SELECT sku_key, seq, op, prev_stock_qty, stock_qty,
                 lag(stock_qty) OVER (PARTITION BY sku_key ORDER BY seq) AS before
          FROM {events_table}
        ),
        per_sku AS (
          SELECT sku_key,
                 sum(CASE WHEN before IS NOT NULL AND NOT (prev_stock_qty <=> before)
                          THEN 1 ELSE 0 END) AS breaks,
                 sum(stock_qty - coalesce(prev_stock_qty, 0)) AS by_deltas,
                 max_by(stock_qty, seq) AS after_image,
                 max_by(op, seq) AS last_op,
                 sum(CASE WHEN prev_stock_qty > stock_qty THEN prev_stock_qty - stock_qty ELSE 0 END) AS sold
          FROM ordered GROUP BY sku_key
        )
        SELECT sum(breaks) AS breaks,
               sum(CASE WHEN breaks > 0 THEN 1 ELSE 0 END) AS broken_skus,
               sum(CASE WHEN last_op <> 'D' AND after_image <> by_deltas THEN 1 ELSE 0 END) AS off_skus,
               sum(CASE WHEN last_op <> 'D' THEN abs(after_image - by_deltas) ELSE 0 END) AS off_units,
               sum(sold) AS units_sold
        FROM per_sku
        """
    ).first()


def change_counts(table, start_version=0):
    """{version: {change type: rows}} from the Change Data Feed."""
    counts = {}
    for r in spark.sql(
        f"SELECT _commit_version AS v, _change_type AS t, count(*) AS n "
        f"FROM table_changes('{table}', {start_version}) GROUP BY 1, 2"
    ).collect():
        counts.setdefault(r.v, {})[r.t] = r.n
    return counts


def latest_version(table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first().version