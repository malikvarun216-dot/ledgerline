# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver — orders: one row per order, filled in as its events arrive
# MAGIC
# MAGIC **Pattern:** a stream from Bronze `orders` → `foreachBatch` → one copy per event → refuse a step
# MAGIC with two different times → fold the batch to one row per order → `MERGE INTO silver.orders ON
# MAGIC order_id` (each event fills its own step column) + an insert-only `MERGE` of the line items.
# MAGIC `Trigger.AvailableNow`: each run applies every Bronze row not yet seen, then stops. The code is in
# MAGIC `_merge_orders`.
# MAGIC
# MAGIC **Safe to "Run all" at any time.** The checkpoint remembers which Bronze rows were applied; a re-run
# MAGIC with nothing new writes nothing, and a replayed batch changes nothing.
# MAGIC
# MAGIC **First run only:** the stream starts at Bronze **version 0**, three files a batch, so it reads
# MAGIC Bronze's history in the order Bronze wrote it (8 batches of ~50,000 messages, then Drill 1's 74
# MAGIC re-sends). An order created in one batch and delivered in a later one is inserted, then updated —
# MAGIC the UPDATE clause runs on live data. This needs the files Bronze's automatic OPTIMIZE replaced on
# MAGIC 2026-09-27, kept 30 days: a **new generation after ~2026-10-27 must drop `starting_version`**.
# MAGIC
# MAGIC Every expected number below was computed on the laptop from the raw Olist files and is pinned in
# MAGIC `tests/test_headline_numbers.py` — a different engine and a different code path from this notebook.

# COMMAND ----------

# MAGIC %run ./_merge_orders

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.silver.checkpoints")

# Same rule as Bronze and Silver CDC: the checkpoint and the app id share a generation, and a
# checkpoint is never deleted in place. A rebuild is a new GENERATION.
GENERATION = "v1"
CHECKPOINT = f"{CHECKPOINTS}/orders/{GENERATION}"
APP_ID = f"ledgerline.silver.orders.{GENERATION}"

run_silver_orders(checkpoint=CHECKPOINT, app_id=APP_ID, starting_version=0, max_files_per_batch=3)
display(spark.table(ORDERS_LOG).orderBy("batch_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify 1 — every Bronze row reached Silver once; every order once; every unit once
# MAGIC
# MAGIC Bronze holds 394,164 messages: 394,090 events, plus 74 labelled re-sends from Drill 1 (the same
# MAGIC 37 events, twice).

# COMMAND ----------

log = spark.table(ORDERS_LOG)
totals = log.agg(
    *[F.sum(c).alias(c) for c in ("bronze_rows", "duplicate_copies", "events", "inserted", "updated",
                                  "items_inserted")],
    F.count(F.lit(1)).alias("batches"),
    F.count_distinct("batch_id").alias("batch_ids"),
).first()
bronze_rows = spark.table(BRONZE_ORDERS).count()
orders_rows = spark.table(ORDERS).agg(
    F.count(F.lit(1)).alias("rows"), F.count_distinct("order_id").alias("ids")
).first()
items_rows = spark.table(ITEMS).count()
print(f"orders log: {totals.asDict()}")
print(f"Bronze rows {bronze_rows:,}; silver.orders {orders_rows.asDict()}; silver.order_items {items_rows:,}")

assert totals.batches == totals.batch_ids, "a batch logged twice"
assert totals.bronze_rows == bronze_rows == 394_164, "a Bronze row was skipped or applied twice"
assert orders_rows.rows == orders_rows.ids == totals.inserted == 99_441
assert items_rows == totals.items_inserted == 112_650
assert totals.updated > 0, "the UPDATE clause never ran — every order arrived complete in one batch"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify 2 — Silver equals the laptop's answer key, column by column
# MAGIC
# MAGIC For each step: how many orders have it, and the sum of its times in Unix seconds (a checksum —
# MAGIC one moved timestamp changes it). Then statuses, units, items, money.

# COMMAND ----------

ANSWER_KEY = {
    "created": (99_441, 150_624_256_503_496),
    "approved": (99_281, 150_385_430_521_853),
    "shipped": (97_658, 147_961_888_552_408),
    "delivered": (96_476, 146_251_032_384_713),
    "canceled": (625, 945_845_962_563),
    "unavailable": (609, 917_417_673_520),
}
STATUSES = {"delivered": 96_470, "shipped": 1_114, "canceled": 625, "approved": 618, "unavailable": 609,
            "created": 5}

orders = spark.table(ORDERS)
got = orders.agg(
    *[F.count(f"{s}_at").alias(f"{s}_n") for s in STEPS],
    *[F.sum(F.unix_seconds(f"{s}_at")).alias(f"{s}_sum") for s in STEPS],
    F.count("estimated_delivery_at").alias("est_n"),
    F.sum(F.unix_seconds("estimated_delivery_at")).alias("est_sum"),
    F.sum("units").alias("units"),
    F.count_if(F.col("units") == 0).alias("no_items"),
    F.count_if(F.col("units").isNull()).alias("units_null"),
).first()
steps = {s: (got[f"{s}_n"], got[f"{s}_sum"]) for s in STEPS}
statuses = {r.status: r.n for r in orders.groupBy("status").agg(F.count(F.lit(1)).alias("n")).collect()}
items = spark.table(ITEMS).agg(
    F.count(F.lit(1)).alias("rows"),
    F.count_distinct("sku_key").alias("skus"),
    F.round(F.sum("price"), 2).alias("price"),
    F.round(F.sum("freight_value"), 2).alias("freight"),
).first()
print(f"steps {steps}\nstatuses {statuses}")
print(f"estimated {got.est_n:,} / {got.est_sum:,}; units {got.units:,}; no items {got.no_items}; "
      f"units null {got.units_null}; items {items.asDict()}")

assert steps == ANSWER_KEY
assert statuses == STATUSES
assert (got.est_n, got.est_sum) == (99_441, 150_828_461_078_400)
assert (got.units, got.no_items, got.units_null) == (112_650, 775, 0)
assert (items.rows, items.skus) == (112_650, 34_448)
assert (float(items.price), float(items.freight)) == (13_591_643.70, 2_251_909.54)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify 3 — every batch inserted and updated exactly what Bronze's own batches predict
# MAGIC
# MAGIC Recomputed from Bronze alone, with Bronze's `_batch_id`: an order is inserted by the first Bronze
# MAGIC batch holding one of its events and updated by every later one. A MERGE that updated rows for
# MAGIC nothing (a re-send, a step already held) would show more updates here than predicted. Exact per
# MAGIC batch only when each Silver batch read exactly one Bronze batch — checked, not assumed.

# COMMAND ----------

expected = {r._batch_id: (r.inserted, r.updated) for r in expected_by_bronze_batch().collect()}
lines = log.orderBy("batch_id").collect()
aligned = all(r.bronze_batch_min == r.bronze_batch_max for r in lines) and len(
    {r.bronze_batch_min for r in lines}
) == len(lines)
for r in lines:
    want = expected.get(r.bronze_batch_min, (0, 0)) if aligned else "-"
    print(f"silver batch {r.batch_id} <- bronze {r.bronze_batch_min}..{r.bronze_batch_max}: "
          f"rows {r.bronze_rows:,} inserted {r.inserted:,} updated {r.updated:,}  expected {want}")
print(f"expected over all of Bronze: inserted {sum(v[0] for v in expected.values()):,} "
      f"updated {sum(v[1] for v in expected.values()):,}")

assert sum(v[0] for v in expected.values()) == 99_441
if aligned:
    assert all((r.inserted, r.updated) == expected.get(r.bronze_batch_min, (0, 0)) for r in lines)
    assert totals.updated == sum(v[1] for v in expected.values())
else:
    print("NOT aligned: a Silver batch read more than one Bronze batch; only the totals are checked")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify 4 — the change feed agrees with the orders log, batch by batch

# COMMAND ----------

order_feed = change_counts(ORDERS)
item_feed = change_counts(ITEMS)
for r in lines:
    o = order_feed.get(r.orders_version, {})
    i = item_feed.get(r.items_version, {})
    seen = (o.get("insert", 0), o.get("update_postimage", 0), i.get("insert", 0))
    print(f"batch {r.batch_id}: log {(r.inserted, r.updated, r.items_inserted)}  feed {seen}")
    assert seen == (r.inserted, r.updated, r.items_inserted)
    assert o.get("update_preimage", 0) == r.updated
deletes = sum(v.get("delete", 0) for feed in (order_feed, item_feed) for v in feed.values())
assert deletes == 0, "an order or item was deleted; nothing in this pipeline deletes"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify 5 — why one column per step: what "the last event wins" would have said
# MAGIC
# MAGIC The rejected design, computed read-only from Bronze: each order's status = the type of the last of
# MAGIC its events to arrive. It disagrees with Silver on **93** orders, because Olist's own timestamps run
# MAGIC backwards on some of them (approved after delivered). Plus the two facts that ruled out the
# MAGIC constraints and the `seq`-style guard: 166 orders arrive with `shipped` first, and 1,305 orders have
# MAGIC two steps at one timestamp. And the status Olist printed on the row disagrees on 623 orders —
# MAGIC statuses no event carries (`invoiced`, `processing`), kept as `source_status`, not used.

# COMMAND ----------

first_copy = Window.partitionBy("event_id").orderBy(*COORDINATES)
events = (
    spark.table(BRONZE_ORDERS)
    .withColumn("_copy", F.row_number().over(first_copy))
    .where("_copy = 1")
)
arrival = events.groupBy("order_id").agg(
    F.max_by("event_type", "_kafka_offset").alias("last_type"),
    F.min_by("event_type", "_kafka_offset").alias("first_type"),
    (F.count_distinct("event_ts") < F.count(F.lit(1))).alias("shared_time"),
)
compared = arrival.join(orders.select("order_id", "status", "source_status"), "order_id")
last_wins = {
    (r.last_type, r.status): r.n
    for r in compared.where("last_type <> status").groupBy("last_type", "status")
    .agg(F.count(F.lit(1)).alias("n")).collect()
}
source = {
    (r.source_status, r.status): r.n
    for r in compared.where("source_status <> status").groupBy("source_status", "status")
    .agg(F.count(F.lit(1)).alias("n")).collect()
}
first_not_created = compared.where("first_type <> 'created'").count()
shared_time = compared.where("shared_time").count()
print(f"last event wins, differs: {sum(last_wins.values())} {last_wins}")
print(f"source_status differs: {sum(source.values())} {source}")
print(f"first event not created: {first_not_created}; two steps at one time: {shared_time}")

assert last_wins == {("approved", "delivered"): 61, ("shipped", "delivered"): 23, ("approved", "shipped"): 9}
assert source == {("invoiced", "approved"): 314, ("processing", "approved"): 301,
                  ("delivered", "shipped"): 7, ("delivered", "approved"): 1}
assert (first_not_created, shared_time) == (166, 1_305)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify 6 — the refusal has something to catch, and nothing real trips it
# MAGIC
# MAGIC Over all of Bronze, the stream's own `find_problems`: nothing. Then one real `created` event copied
# MAGIC with its time moved by an hour (a new `event_id`, as the generator would make): next to the original
# MAGIC it is two events for one step; alone, against Silver, it is a step whose time changed. Read-only.

# COMMAND ----------

assert find_problems(clean_events(spark.table(BRONZE_ORDERS)), ORDERS) == {}

# Ordered before limit(1): each branch of the union below evaluates it on its own, and an unordered
# limit could pick a different order in each.
real = events.where("event_type = 'created'").orderBy("event_id").limit(1)
moved = real.withColumn(
    "event_id", F.sha2(F.concat(F.col("event_id"), F.lit("-moved")), 256).substr(1, 32)
).withColumn("event_ts", F.col("event_ts") + F.expr("INTERVAL 1 HOUR"))
in_batch = find_problems(real.unionByName(moved), ORDERS)
vs_silver = find_problems(moved, ORDERS)
print(f"original + moved copy: {in_batch}\nmoved copy alone: {vs_silver}")
assert in_batch == {"two_events_one_step": 1, "step_time_changed": 1}
assert vs_silver == {"step_time_changed": 1}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify 7 — the two sources agree, per SKU: units ordered = units the stock feed decremented
# MAGIC
# MAGIC The project's cross-source reconciliation, now between two Silver tables built from two topics.
# MAGIC Per SKU, not only the total: a total can hide two SKUs off by opposite amounts.

# COMMAND ----------

recon = spark.sql(
    f"""
    WITH ordered AS (SELECT sku_key, count(*) AS units FROM {ITEMS} GROUP BY sku_key),
    decremented AS (
      SELECT sku_key, sum(prev_stock_qty - stock_qty) AS units
      FROM {SILVER}.inventory_events WHERE prev_stock_qty > stock_qty GROUP BY sku_key
    )
    SELECT count(*) AS skus,
           count_if(coalesce(o.units, 0) <> coalesce(d.units, 0)) AS mismatched,
           sum(o.units) AS ordered, sum(d.units) AS decremented
    FROM ordered o FULL OUTER JOIN decremented d USING (sku_key)
    """
).first()
print(recon.asDict())
assert (recon.skus, recon.mismatched, recon.ordered, recon.decremented) == (34_448, 0, 112_650, 112_650)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The constraints Delta now enforces on these tables

# COMMAND ----------

for table in (ORDERS, ITEMS):
    for r in spark.sql(f"SHOW TBLPROPERTIES {table}").where("key LIKE 'delta.constraints.%'").collect():
        print(f"{table}: {r.key} = {r.value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plain re-run — nothing new in Bronze, so nothing may be written

# COMMAND ----------


def own_commits(table):
    """Commits this pipeline makes. Databricks' own OPTIMIZE / VACUUM are left out (Session 8)."""
    return spark.sql(f"DESCRIBE HISTORY {table}").where(
        "operation IN ('MERGE', 'WRITE', 'CREATE TABLE', 'STREAMING UPDATE', 'ADD CONSTRAINT')"
    ).count()


before = {t: own_commits(t) for t in (ORDERS, ITEMS, ORDERS_LOG)}
run_silver_orders(checkpoint=CHECKPOINT, app_id=APP_ID, starting_version=0, max_files_per_batch=3)
after = {t: own_commits(t) for t in (ORDERS, ITEMS, ORDERS_LOG)}
print(f"commits before {before}\nafter  {after}")
assert before == after, "a re-run with nothing new wrote to Silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look at what happened

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {ORDERS}").select(
        "version", "timestamp", "operation", "operationParameters", "operationMetrics"
    )
)