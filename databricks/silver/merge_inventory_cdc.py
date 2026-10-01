# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver — inventory: every change once, and the stock as it is now
# MAGIC
# MAGIC **Pattern:** a stream from Bronze `inventory_cdc` → `foreachBatch` → denylist, dedup, tie refusal →
# MAGIC an insert-only event log + the CDC `MERGE` on op flags with a `seq` guard. `Trigger.AvailableNow`:
# MAGIC each run applies every Bronze row not yet seen, then stops. The code is in `_merge_inventory_cdc`.
# MAGIC
# MAGIC **Safe to "Run all" at any time.** The checkpoint remembers which Bronze rows were applied; a re-run
# MAGIC with nothing new writes nothing, and a replayed batch changes nothing (every write is repeatable).
# MAGIC
# MAGIC **If a batch is refused** (two different events at one `seq` for a SKU), nothing is written and the
# MAGIC stream stops on that batch. A person finds the wrong events and adds them to the denylist file in
# MAGIC `ops/incidents/`, in a commit; the next run retries the same batch.
# MAGIC
# MAGIC Every expected number below was computed on the laptop with pandas from the raw Olist files
# MAGIC (`build_cdc_events`) — a different engine and a different code path from this notebook.

# COMMAND ----------

# MAGIC %run ./_merge_inventory_cdc

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.silver.checkpoints")

# Same rule as Bronze: the checkpoint and the app id share a generation, and a checkpoint is never
# deleted in place. A rebuild is a new GENERATION.
GENERATION = "v1"
CHECKPOINT = f"{CHECKPOINTS}/inventory/{GENERATION}"
APP_ID = f"ledgerline.silver.inventory.{GENERATION}"

DENYLIST = load_denylist()
assert len(DENYLIST) == 53, f"denylist has {len(DENYLIST)} ids, expected 53"

run_silver_cdc(checkpoint=CHECKPOINT, app_id=APP_ID, denylist=DENYLIST)
display(spark.table(CDC_LOG).orderBy("batch_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify 1 — every Bronze row reached Silver once; the log is Bronze minus the denylist
# MAGIC
# MAGIC Bronze holds 158,625 messages: 158,346 real events, 120 exact re-sends, 159 denylisted (53 ids, three
# MAGIC copies each). After Session 9's delist run, 100 more: one `D` per delisted SKU.

# COMMAND ----------

totals = spark.table(CDC_LOG).agg(
    *[F.sum(c).alias(c) for c in (
        "bronze_rows", "denied_rows", "events_inserted", "inserted", "updated", "deleted", "chain_breaks",
    )],
    F.count(F.lit(1)).alias("batches"),
).first()
bronze_rows = spark.table(BRONZE_CDC).count()
logged = spark.table(EVENTS).agg(
    F.count(F.lit(1)).alias("rows"),
    F.count_distinct("event_id").alias("distinct_ids"),
    F.sum((F.col("op") == "D").cast("int")).alias("deletes"),
    F.sum(F.col("event_id").isin(DENYLIST).cast("int")).alias("denied"),
).first()
DELISTS = logged.deletes or 0
print(f"CDC log: {totals.batches} batch(es) {totals.asDict()}")
print(f"Bronze rows {bronze_rows:,}; event log {logged.asDict()}")

assert DELISTS in (0, 100), f"{DELISTS} delete events in the log; the delist run sends exactly 100"
assert totals.bronze_rows == bronze_rows, "a Bronze row was skipped or applied twice"
assert totals.denied_rows == 159
assert logged.rows == logged.distinct_ids == 158_346 + DELISTS
assert logged.denied == 0
assert totals.events_inserted == logged.rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify 2 — the stock equals the laptop's answer key, and a batch recomputation from the log
# MAGIC
# MAGIC Two independent answers. The laptop's: SKU count and total stock from pandas. Spark's: each SKU's
# MAGIC highest-`seq` event from the log in one window query — no MERGE, no micro-batches — compared with
# MAGIC Silver on every column, both ways.

# COMMAND ----------

ANSWER_KEY = {0: (34_448, 993_982), 100: (34_348, 991_530)}  # delists -> (SKUs, total stock)

stock = spark.table(STOCK).agg(
    F.count(F.lit(1)).alias("rows"),
    F.count_distinct("sku_key").alias("skus"),
    F.sum("stock_qty").alias("units"),
).first()
extra, missing = compare_stock(STOCK, expected_stock())
print(f"stock: {stock.asDict()}  expected (skus, units) {ANSWER_KEY[DELISTS]}")
print(f"vs the log's newest events: extra={extra} missing={missing}")

assert stock.rows == stock.skus, "a SKU appears twice in the stock"
assert (stock.skus, stock.units) == ANSWER_KEY[DELISTS]
assert (extra, missing) == (0, 0), "the streamed stock differs from the log's newest events"
assert totals.inserted - totals.deleted == stock.rows
assert totals.deleted == DELISTS

if DELISTS:
    batch = spark.table(CDC_LOG).where("newest_is_delete > 0").collect()
    assert len(batch) == 1, f"the delists arrived in {len(batch)} batches"
    d = batch[0]
    got = (d.events_inserted, d.newest_is_delete, d.deleted, d.inserted, d.updated, d.chain_breaks)
    print(f"delist batch {d.batch_id}: (events, newest D, deleted, inserted, updated, breaks) = {got}")
    assert got == (100, 100, 100, 0, 0, 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify 3 — the known source defect, exactly as measured on the laptop, and nothing new
# MAGIC
# MAGIC The feed's before/after images break their own `seq` order on 237 SKUs (incidents.md, 2026-10-01).
# MAGIC Silver keeps the after-image of each SKU's newest event anyway (decisions.md, Session 9), so 26 SKUs
# MAGIC hold a stock 27 units in total away from what their deltas add up to. Pinned: a different number in
# MAGIC either direction means the feed or the code changed. Units sold is the Session 6 tie-out.

# COMMAND ----------

report = chain_report()
print(report.asDict())
assert (report.breaks, report.broken_skus) == (1_423, 237)
assert (report.off_skus, report.off_units) == (26, 27)
assert report.units_sold == 112_650
assert totals.chain_breaks == 1_423, "per-batch counts do not add up to the whole-log count"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify 4 — the change feed agrees with the CDC log, batch by batch

# COMMAND ----------

feed = change_counts(STOCK)
for r in spark.table(CDC_LOG).orderBy("batch_id").collect():
    got = feed.get(r.stock_version, {})
    seen = (got.get("insert", 0), got.get("update_postimage", 0), got.get("delete", 0))
    print(f"batch {r.batch_id} v{r.stock_version}: log {(r.inserted, r.updated, r.deleted)}  feed {seen}")
    assert seen == (r.inserted, r.updated, r.deleted)
    assert got.get("update_preimage", 0) == r.updated

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify 5 — the tie guard has something real to catch
# MAGIC
# MAGIC The same `find_problems` the stream runs, over all of Bronze, read-only. **Without** the denylist the
# MAGIC 2026-09-26 contamination must show as ties — otherwise the guard has never been shown able to fire.
# MAGIC **With** it, nothing.

# COMMAND ----------

everything = spark.table(BRONZE_CDC)
without = find_problems(clean_events(everything, [])[1], EVENTS)
with_denylist = find_problems(clean_events(everything, DENYLIST)[1], EVENTS)
print(f"without the denylist: {without}\nwith it: {with_denylist}")
assert without.get("tied_seq", 0) > 0 and without.get("tied_seq_vs_log", 0) > 0
assert with_denylist == {}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plain re-run — nothing new in Bronze, so nothing may be written

# COMMAND ----------


def own_commits(table):
    """Commits this pipeline makes. Databricks' own OPTIMIZE / VACUUM are left out (Session 8)."""
    return spark.sql(f"DESCRIBE HISTORY {table}").where(
        "operation IN ('MERGE', 'WRITE', 'CREATE TABLE', 'STREAMING UPDATE')"
    ).count()


before = {t: own_commits(t) for t in (EVENTS, STOCK, CDC_LOG)}
run_silver_cdc(checkpoint=CHECKPOINT, app_id=APP_ID, denylist=DENYLIST)
after = {t: own_commits(t) for t in (EVENTS, STOCK, CDC_LOG)}
print(f"commits before {before}\nafter  {after}")
assert before == after, "a re-run with nothing new wrote to Silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look at what happened

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {STOCK}").select(
        "version", "timestamp", "operation", "operationParameters", "operationMetrics"
    )
)