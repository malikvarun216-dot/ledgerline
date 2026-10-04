# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bronze → Silver for order events — shared code
# MAGIC
# MAGIC Loaded with `%run ./_merge_orders` by `merge_orders`. **Defines functions only — running it writes
# MAGIC nothing.**
# MAGIC
# MAGIC Bronze holds every order event exactly as the topic delivered it: one message per lifecycle step
# MAGIC (`created`, `approved`, `shipped`, `delivered`, and a synthetic `canceled` / `unavailable`). Silver
# MAGIC keeps two tables:
# MAGIC - `silver.orders` — **one row per order**, one timestamp column per lifecycle step. Each event fills
# MAGIC   its own column; `status` is the furthest step reached. (An *accumulating snapshot*: the row grows
# MAGIC   as the order moves.)
# MAGIC - `silver.order_items` — one row per unit ordered, from the `items` on the `created` event.
# MAGIC
# MAGIC **Why one column per step, not "the latest event wins"** (decisions.md, Session 10): Olist's own
# MAGIC timestamps run backwards on 1,382 orders, so "the last event to arrive" gives the wrong status on 93
# MAGIC (61 say `approved` for an order that was delivered). A column per step does not care what order the
# MAGIC events arrive in — `shipped` before `created` (166 orders) lands the same as after — so there is no
# MAGIC `seq` and no guard to get wrong. The opposite of the CDC MERGE, where order is everything.
# MAGIC
# MAGIC | event in the batch for an order | in Silver? | clause | result |
# MAGIC |---|---|---|---|
# MAGIC | brings a step Silver lacks | yes | `WHEN MATCHED AND (s.x_at NOT NULL, t.x_at NULL …)` | filled |
# MAGIC | only steps Silver already has (a re-send, a replay) | yes | — | nothing |
# MAGIC | anything | no | `WHEN NOT MATCHED` | insert |
# MAGIC | the same step with a **different** time | either | refused before the MERGE | the batch fails |
# MAGIC
# MAGIC Every write is safe to repeat (a replay brings no step Silver lacks; items are insert-only on
# MAGIC `(order_id, order_item_id)`), so no `txnVersion` on the MERGEs. The log line is an append and gets it.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

BRONZE_ORDERS = "workspace.bronze.orders"
SILVER = "workspace.silver"
ORDERS = f"{SILVER}.orders"
ITEMS = f"{SILVER}.order_items"
ORDERS_LOG = f"{SILVER}.orders_log"
CHECKPOINTS = "/Volumes/workspace/silver/checkpoints"

# One timestamp column per event type, `<type>_at`. Olist carries a real timestamp for the first four;
# `canceled` / `unavailable` are placed one second after the last real step (ts_is_synthetic = true).
STEPS = ("created", "approved", "shipped", "delivered", "canceled", "unavailable")
# The status is the first of these that has a time: a terminal event outranks everything, then the
# furthest real step. An order can be canceled after it was delivered (6 orders) — it is canceled.
STATUS_PRECEDENCE = ("canceled", "unavailable", "delivered", "shipped", "approved", "created")

TABLE_PROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
}

# Enforced by Delta on every write: a row that breaks one fails the whole write, nothing committed.
# Only facts that hold at EVERY moment of the stream, not just at the end — "created_at is set" would
# refuse the 166 orders whose `shipped` arrives first, and "shipped after approved" is broken by the
# source itself 1,359 times (decisions.md, Session 10).
ORDER_CONSTRAINTS = {
    "status_known": "status IN ('created', 'approved', 'shipped', 'delivered', 'canceled', 'unavailable')",
}
ITEM_CONSTRAINTS = {
    "money_not_negative": "price >= 0 AND freight_value >= 0",
}

ORDER_FIELDS = ["order_id", "customer_id", "status", "source_status"] + [f"{s}_at" for s in STEPS] + [
    "estimated_delivery_at", "units",
]
ITEM_FIELDS = ["order_id", "order_item_id", "product_id", "seller_id", "sku_key", "price", "freight_value"]
COORDINATES = ["_kafka_partition", "_kafka_offset"]


class BatchRefused(Exception):
    """A batch with two different times for one step of one order. Nothing was written; the next run
    retries the same batch."""


class UnsafeStreamReset(Exception):
    """A stream with no history about to write into an orders log that already has lines (Drill 2)."""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables

# COMMAND ----------


def _properties(table):
    return {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}


def ensure_properties(table):
    """For a table created before a property was chosen. Checked first: every ALTER is a commit."""
    have = _properties(table)
    for name, value in TABLE_PROPERTIES.items():
        if have.get(name) != value:
            spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('{name}' = '{value}')")
            print(f"{table}: set {name} = {value}")


def ensure_constraints(table, constraints):
    """Add each CHECK constraint the table lacks. Delta keeps them as `delta.constraints.<name>` and
    checks every existing row before adding one — a row that breaks it fails the ALTER, nothing changes.
    One that exists with a different rule is refused, never silently replaced."""
    have = _properties(table)

    def canonical(rule):  # Delta may store the rule with other spacing, case, brackets or backticks
        return "".join(ch for ch in rule.lower() if ch not in " ()`")

    for name, rule in constraints.items():
        stored = have.get(f"delta.constraints.{name}")
        if stored is None:
            spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({rule})")
            print(f"{table}: added constraint {name}")
        elif canonical(stored) != canonical(rule):
            raise ValueError(f"{table}: constraint {name} is {stored!r}, the code says {rule!r}")


def create_tables(orders=ORDERS, items=ITEMS, log=ORDERS_LOG):
    """In the notebook's own session, before the stream starts (inside foreachBatch the session is a
    clone, where a catalog lookup once answered wrongly — incidents.md, 2026-09-26)."""
    props = ", ".join(f"'{k}' = '{v}'" for k, v in TABLE_PROPERTIES.items())
    steps = ", ".join(f"{s}_at TIMESTAMP" for s in STEPS)
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {orders} (
            order_id STRING NOT NULL, customer_id STRING, status STRING, source_status STRING,
            {steps}, estimated_delivery_at TIMESTAMP, units INT, _merged_at TIMESTAMP
        ) TBLPROPERTIES ({props})"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {items} (
            order_id STRING NOT NULL, order_item_id INT NOT NULL, product_id STRING, seller_id STRING,
            sku_key STRING, price DOUBLE, freight_value DOUBLE,
            _event_id STRING, _kafka_partition INT, _kafka_offset BIGINT, _merged_at TIMESTAMP
        ) TBLPROPERTIES ({props})"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {log} (
            batch_id BIGINT, bronze_rows BIGINT, duplicate_copies BIGINT, events BIGINT, orders BIGINT,
            inserted BIGINT, updated BIGINT, items_inserted BIGINT,
            bronze_batch_min BIGINT, bronze_batch_max BIGINT,
            orders_version BIGINT, items_version BIGINT, applied_at TIMESTAMP
        )"""
    )
    for table, constraints in ((orders, ORDER_CONSTRAINTS), (items, ITEM_CONSTRAINTS)):
        ensure_properties(table)
        ensure_constraints(table, constraints)

# COMMAND ----------

# MAGIC %md
# MAGIC ## One micro-batch, step by step
# MAGIC
# MAGIC Every function below takes its Spark session from the DataFrame it is given, never the notebook's
# MAGIC `spark`: inside `foreachBatch` they run in the batch's own session.

# COMMAND ----------


def clean_events(batch, denylist=()):
    """Bronze rows -> one copy per `event_id` (the lowest Kafka offset, so a replay keeps the same copy),
    minus any event a person has denylisted. Orders have no denylist today; the parameter is the way out
    a refused batch needs."""
    first_copy = Window.partitionBy("event_id").orderBy(*COORDINATES)
    events = batch
    if denylist:
        events = events.where(~F.col("event_id").isin(list(denylist)))
    return (
        events.withColumn("_copy", F.row_number().over(first_copy))
        .where("_copy = 1")
        .drop("_copy")
    )


def per_order(events):
    """The batch's events folded into one row per order — the in-batch dedup for this MERGE.

    Not `row_number() ... = 1` as in the CDC path: there the newest event replaces the others; here every
    event of an order carries a different column, and all of them are kept. An aggregation keeps them all
    and still leaves one source row per order, so the MERGE never sees an order twice.
    """
    step_times = [
        F.max(F.when(F.col("event_type") == s, F.col("event_ts"))).alias(f"{s}_at") for s in STEPS
    ]
    units = F.when(F.col("items").isNull(), F.lit(0)).otherwise(F.size("items"))
    return events.groupBy("order_id").agg(
        # The same on every event of an order (laptop: 0 orders differ); max() only makes the pick stable.
        F.max("customer_id").alias("customer_id"),
        F.max("order_status").alias("source_status"),
        F.max("estimated_delivery_date").alias("estimated_delivery_at"),
        *step_times,
        F.max(F.when(F.col("event_type") == "created", units)).alias("units"),
    )


def order_items(events):
    """One row per unit, from the `created` event's items. `created` is the only event that carries them."""
    item = F.col("i")
    return (
        events.where("event_type = 'created' AND items IS NOT NULL")
        .select("order_id", "event_id", *COORDINATES, F.explode("items").alias("i"))
        .select(
            "order_id",
            item.order_item_id.alias("order_item_id"),
            item.product_id.alias("product_id"),
            item.seller_id.alias("seller_id"),
            F.concat_ws("|", item.product_id, item.seller_id).alias("sku_key"),
            item.price.alias("price"),
            item.freight_value.alias("freight_value"),
            F.col("event_id").alias("_event_id"),
            *COORDINATES,
        )
    )


def find_problems(events, orders_table):
    """Every way this batch contradicts itself or Silver. Must be empty. Reads only.

    - `two_events_one_step`: two different events for the same step of one order, inside the batch
      (`event_id` hashes the time, so a different time is a different event);
    - `step_time_changed`: the batch's time for a step differs from the one Silver already holds;
    - `unknown_event_type`: a type with no column (it would be silently dropped by the fold).
    No rule can say which time is right; keeping either one in silence is the Session 5 lesson.
    """
    s = events.sparkSession
    in_batch = (
        events.groupBy("order_id", "event_type").agg(F.count_distinct("event_id").alias("n"))
        .where("n > 1").count()
    )
    batch = per_order(events).alias("b")
    silver = s.table(orders_table).alias("t")
    changed = F.lit(False)
    for step in STEPS:
        c = f"{step}_at"
        changed = changed | (
            F.col(f"b.{c}").isNotNull() & F.col(f"t.{c}").isNotNull() & (F.col(f"b.{c}") != F.col(f"t.{c}"))
        )
    vs_silver = batch.join(silver, "order_id").where(changed).count()
    unknown = events.where(F.col("event_type").isNull() | ~F.col("event_type").isin(*STEPS)).count()
    found = {"two_events_one_step": in_batch, "step_time_changed": vs_silver, "unknown_event_type": unknown}
    return {k: v for k, v in found.items() if v}


def status_sql(column):
    """`status` from the step times: the first in STATUS_PRECEDENCE that has one."""
    whens = " ".join(f"WHEN {column(s)} IS NOT NULL THEN '{s}'" for s in STATUS_PRECEDENCE)
    return f"CASE {whens} END"


def orders_merge_sql(target, source):
    """The accumulating-snapshot MERGE: each step fills its own column, once.

    The MATCHED clause fires only when the batch brings a step Silver does not have yet — without that
    condition every re-sent event would rewrite its order's row for nothing (exp_03 F: 2,995 updates
    instead of 50). `coalesce(t, s)` keeps a time already held; find_problems has already refused a batch
    whose time differs.
    """
    brings_new = " OR ".join(f"(s.{s}_at IS NOT NULL AND t.{s}_at IS NULL)" for s in STEPS)
    merged = status_sql(lambda step: f"coalesce(t.{step}_at, s.{step}_at)")
    sets = [f"t.{s}_at = coalesce(t.{s}_at, s.{s}_at)" for s in STEPS] + [
        "t.units = coalesce(t.units, s.units)",
        f"t.status = {merged}",
        "t._merged_at = current_timestamp()",
    ]
    cols = [c for c in ORDER_FIELDS if c != "status"]
    return (
        f"MERGE INTO {target} AS t\n"
        f"USING {source} AS s\n"
        f"ON t.order_id = s.order_id\n"
        f"WHEN MATCHED AND ({brings_new}) THEN UPDATE SET {', '.join(sets)}\n"
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}, status, _merged_at) "
        f"VALUES ({', '.join('s.' + c for c in cols)}, {status_sql(lambda step: f's.{step}_at')}, "
        f"current_timestamp())"
    )


def items_merge_sql(target, source):
    """Insert-only on (order_id, order_item_id): a `created` event already applied adds nothing."""
    cols = [*ITEM_FIELDS, "_event_id", *COORDINATES]
    return (
        f"MERGE INTO {target} AS t\n"
        f"USING {source} AS s\n"
        f"ON t.order_id = s.order_id AND t.order_item_id = s.order_item_id\n"
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}, _merged_at) "
        f"VALUES ({', '.join('s.' + c for c in cols)}, current_timestamp())"
    )


def _version(s, table):
    return s.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first().version


def _merge(s, table, sql):
    """Run one MERGE; what it did, read from the table's own history — the MERGE commit after the
    version we started from, never "the latest version" (Delta adds OPTIMIZE commits of its own).
    Same helper as `_merge_inventory_cdc._merge`."""
    before = _version(s, table)
    s.sql(sql)
    m = (
        s.sql(f"DESCRIBE HISTORY {table}")
        .where(f"version > {before} AND operation = 'MERGE'")
        .orderBy("version")
        .first()
    )
    if m is None:
        return {"inserted": 0, "updated": 0, "version": None}
    metrics = m.operationMetrics
    return {
        "inserted": int(metrics.get("numTargetRowsInserted", 0)),
        "updated": int(metrics.get("numTargetRowsUpdated", 0)),
        "version": m.version,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## The batch function and the stream

# COMMAND ----------


def make_batch_writer(*, app_id, denylist=(), orders_table=ORDERS, items_table=ITEMS, log_table=ORDERS_LOG):
    """Return the foreachBatch function. Nothing printed in here reaches the notebook on serverless
    (incidents.md, Drill 1): the evidence is the orders log and the tables' history."""
    denylist = list(denylist)

    def apply_batch(batch_df, batch_id):
        s = batch_df.sparkSession
        events = clean_events(batch_df, denylist)

        problems = find_problems(events, orders_table)
        if problems:
            raise BatchRefused(
                f"batch {batch_id}: {problems}. Nothing was written. Two different times for one step of "
                "an order; a person decides which event is wrong and denylists it. The next run retries "
                "this batch."
            )

        denied = F.col("event_id").isin(denylist) if denylist else F.lit(False)
        seen = batch_df.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(denied.cast("int")).alias("denied"),
            F.min("_batch_id").alias("bronze_min"),
            F.max("_batch_id").alias("bronze_max"),
        ).first()
        size = events.agg(
            F.count(F.lit(1)).alias("events"), F.count_distinct("order_id").alias("orders")
        ).first()

        per_order(events).createOrReplaceTempView("_order_batch")
        applied = _merge(s, orders_table, orders_merge_sql(orders_table, "_order_batch"))
        order_items(events).createOrReplaceTempView("_item_batch")
        items = _merge(s, items_table, items_merge_sql(items_table, "_item_batch"))

        def n(value, name):
            return F.lit(value).cast("bigint").alias(name)

        line = s.range(1).select(
            n(batch_id, "batch_id"),
            n(seen.rows, "bronze_rows"),
            n(seen.rows - (seen.denied or 0) - size.events, "duplicate_copies"),
            n(size.events, "events"),
            n(size.orders, "orders"),
            n(applied["inserted"], "inserted"),
            n(applied["updated"], "updated"),
            n(items["inserted"], "items_inserted"),
            n(seen.bronze_min, "bronze_batch_min"),
            n(seen.bronze_max, "bronze_batch_max"),
            n(applied["version"], "orders_version"),
            n(items["version"], "items_version"),
            F.current_timestamp().alias("applied_at"),
        )
        # An append is NOT safe to repeat: Delta remembers (app id, last batch) and skips a replayed line.
        (
            line.write.format("delta").mode("append")
            .option("txnAppId", app_id).option("txnVersion", batch_id)
            .saveAsTable(log_table)
        )

    return apply_batch


def _missing_path(error):
    """Same as `_kafka_bronze._missing_path`: on a Volume a missing path raises `INTERNAL: No such file
    or directory`, not "not found"."""
    text = f"{type(error).__name__} {error}".lower()
    return any(s in text for s in ("not found", "filenotfound", "no such file"))


def checkpoint_has_history(checkpoint):
    """True once Spark has planned any batch here (an `offsets/N` file exists). Same as Bronze's."""
    try:
        return any(f.name.isdigit() for f in dbutils.fs.ls(f"{checkpoint}/offsets"))
    except Exception as e:
        if _missing_path(e):
            return False
        raise


def refuse_unsafe_reset(checkpoint, app_id, log_table):
    """A stream with no history may only write into an empty orders log (Drill 2). Same rule and same
    reason as `_merge_inventory_cdc.refuse_unsafe_reset`: the MERGEs are safe to replay, the log line —
    keyed by a batch id that restarts at 0 — is not."""
    if checkpoint_has_history(checkpoint):
        return
    if spark.table(log_table).limit(1).count():
        raise UnsafeStreamReset(
            f"checkpoint {checkpoint} has no history, but {log_table} already has lines. A deleted "
            f"checkpoint with app id {app_id!r} kept would leave the log silent for every replayed batch; "
            "a new app id would log every row twice. Restore the checkpoint, or rebuild: new GENERATION "
            "and an empty log."
        )


def run_silver_orders(
    *,
    checkpoint,
    app_id,
    source=BRONZE_ORDERS,
    orders_table=ORDERS,
    items_table=ITEMS,
    log_table=ORDERS_LOG,
    starting_version=None,
    max_files_per_batch=None,
    denylist=(),
    guard_reset=True,
):
    """One AvailableNow pass: every Bronze row not yet seen, in micro-batches, then stop.

    `starting_version` / `max_files_per_batch` matter only on a stream's FIRST run (after that the
    checkpoint decides). Production starts at Bronze version 0, three files a batch, so Silver reads
    Bronze's history commit by commit, in the order Bronze wrote it — an order created in one Bronze
    batch and delivered in a later one is inserted, then updated. Without them the first run reads the
    whole table as one snapshot, every order arrives complete, and the UPDATE clause never runs
    (decisions.md, Session 10). Needs the files Bronze's OPTIMIZE replaced, kept until VACUUM: 30 days.

    `guard_reset=False` exists ONLY so a drill can show the checkpoint-reset trap first.
    """
    create_tables(orders_table, items_table, log_table)
    if guard_reset:
        refuse_unsafe_reset(checkpoint, app_id, log_table)
    reader = spark.readStream
    if starting_version is not None:
        reader = reader.option("startingVersion", starting_version)
    if max_files_per_batch:
        reader = reader.option("maxFilesPerTrigger", max_files_per_batch)
    query = (
        reader.table(source)
        .writeStream.foreachBatch(
            make_batch_writer(
                app_id=app_id,
                denylist=denylist,
                orders_table=orders_table,
                items_table=items_table,
                log_table=log_table,
            )
        )
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks — each one a different code path from the stream

# COMMAND ----------


def expected_by_bronze_batch(source=BRONZE_ORDERS):
    """What each Bronze batch should do to Silver, from Bronze alone — no MERGE, no stream.

    One copy per event; for each order, the Bronze batches that hold one of its events. The first such
    batch inserts the order; every later one brings a step Silver lacks (each step is one event), so it
    updates it. A batch holding only re-sends of events seen earlier does nothing.
    """
    first_copy = Window.partitionBy("event_id").orderBy(*COORDINATES)
    events = (
        spark.table(source)
        .withColumn("_copy", F.row_number().over(first_copy))
        .where("_copy = 1")
    )
    touches = events.select("order_id", "_batch_id").distinct()
    first = Window.partitionBy("order_id").orderBy("_batch_id")
    return (
        touches.withColumn("_nth", F.row_number().over(first))
        .groupBy("_batch_id")
        .agg(
            F.sum((F.col("_nth") == 1).cast("int")).alias("inserted"),
            F.sum((F.col("_nth") > 1).cast("int")).alias("updated"),
        )
        .orderBy("_batch_id")
    )


def change_counts(table, start_version=0):
    """{version: {change type: rows}} from the Change Data Feed."""
    counts = {}
    for r in spark.sql(
        f"SELECT _commit_version AS v, _change_type AS t, count(*) AS n "
        f"FROM table_changes('{table}', {start_version}) GROUP BY 1, 2"
    ).collect():
        counts.setdefault(r.v, {})[r.t] = r.n
    return counts