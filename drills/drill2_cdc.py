# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Drill 2 — Silver inventory attacked: a checkpoint reset, history backwards, a replayed batch
# MAGIC
# MAGIC Silver is finished (dims, inventory, orders). A drill builds nothing new for the pipeline's sake: it
# MAGIC attacks each guarantee on purpose and asserts what happens — the bug first where there is one
# MAGIC (decisions.md, "Drill sessions", Session 6). This notebook takes Silver inventory. The production
# MAGIC code (`_merge_inventory_cdc`) on scratch tables `workspace.silver.drill2_*`, rebuilt every run;
# MAGIC production Bronze and Silver are only read.
# MAGIC
# MAGIC | Part | Attack | Expected |
# MAGIC |---|---|---|
# MAGIC | R1 | checkpoint deleted, **same** app id, 100 rows waiting | applied; not logged; false lag alarm |
# MAGIC | R2 | then a **new** app id "to fix the log" | every row logged twice; lag alarm blind |
# MAGIC | G | the reset guard | both refused before a stream starts |
# MAGIC | B1 | Bronze's batches **newest first**, production code | stock = production; chain count drops |
# MAGIC | B2 | the same, Session 9's code (newest from the batch) | the 100 delisted SKUs come back |
# MAGIC | B3 | the last batch again, same batch id | nothing changes |
# MAGIC | C | a new broken before/after link (incident 2026-10-01) | counted: 1 |
# MAGIC
# MAGIC Steps that must fail never run a stream (incidents.md 2026-09-27, 2026-10-02): the guard refuses
# MAGIC before `readStream`, and B calls the stream's own batch function directly, in an order no stream can
# MAGIC read.

# COMMAND ----------

# MAGIC %run ../databricks/silver/_merge_inventory_cdc

# COMMAND ----------

import uuid

RUN = uuid.uuid4().hex[:8]  # checkpoints are never reused: every run of this notebook gets new ones
DENYLIST = load_denylist()
assert len(DENYLIST) == 53
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.silver.checkpoints")
PRODUCTION_CHECKPOINTS = [f"{CHECKPOINTS}/inventory/v1", f"{CHECKPOINTS}/orders/v1"]


def scratch(part):
    t = {k: f"{SILVER}.drill2_{part}_{k}" for k in ("bronze", "events", "stock", "log")}
    for name in t.values():
        spark.sql(f"DROP TABLE IF EXISTS {name}")
    t["app_id"] = f"ledgerline.drill2.{RUN}.{part}"
    t["checkpoint"] = f"{CHECKPOINTS}/drill2/{RUN}/{part}"
    return t


def lines(t):
    return spark.table(t["log"]).orderBy("applied_at", "batch_id").collect()


def stock_totals(t):
    stock = spark.table(t["stock"])
    r = stock.agg(F.count(F.lit(1)).alias("skus"), F.sum("stock_qty").alias("units")).first()
    return r.skus, r.units


def commits(table, operation):
    history = spark.sql(f"DESCRIBE HISTORY {table}")
    return history.where(f"operation = '{operation}'").orderBy("version").collect()


def forget_checkpoint(checkpoint):
    """What the operator in this trap does: delete the stream's checkpoint. Scratch only."""
    try:
        dbutils.fs.rm(checkpoint, True)
    except Exception as e:
        if not _missing_path(e):
            raise


def repo_file(*parts):
    """A file from this Git folder, found upwards from the notebook (like load_denylist)."""
    here = os.getcwd()
    for _ in range(4):
        path = os.path.join(here, *parts)
        if os.path.exists(path):
            with open(path) as fh:
                return fh.read()
        here = os.path.dirname(here)
    raise FileNotFoundError(os.path.join(*parts))


ALERT_SQL = repo_file("databricks", "alerts", "silver_behind_bronze.sql")


def lag_alarm(bronze_cdc, cdc_log):
    """The production alert query, word for word, with the CDC tables swapped for scratch ones. The dims
    and orders parts still read production (healthy: 0)."""
    sql = ALERT_SQL.replace("workspace.bronze.inventory_cdc", bronze_cdc).replace(
        "workspace.silver.inventory_cdc_log", cdc_log
    )
    return spark.sql(sql).first().sources_behind


production_alarm = spark.sql(ALERT_SQL).first().sources_behind
print(f"the lag alarm on production today: {production_alarm}")
assert production_alarm == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## R — the checkpoint-reset trap, on Silver
# MAGIC
# MAGIC Drill 1 deleted a Kafka stream's checkpoint and kept its app id: Delta skipped every batch number it
# MAGIC remembered and **74 new messages were lost**, no error. Silver's streams have the same two memories —
# MAGIC Spark's checkpoint and Delta's (app id → last batch, in the log table) — but a different layout of
# MAGIC writes: the MERGEs carry **no** `txnVersion` (each is safe to repeat by its own condition,
# MAGIC decisions.md Session 9); only the one-line log append does. Prediction: **no data lost; the log is.**
# MAGIC
# MAGIC Scratch Bronze = production Bronze CDC **without** the 100 delists. Life 1 applies it. Then the
# MAGIC delists "arrive", and the checkpoint is deleted before the next run.

# COMMAND ----------

R = scratch("r")
spark.sql(f"CREATE TABLE {R['bronze']} AS SELECT * FROM {BRONZE_CDC} WHERE op <> 'D'")
assert spark.table(R["bronze"]).count() == 158_625


def run_r(app_id, **switches):
    run_silver_cdc(
        checkpoint=R["checkpoint"], app_id=app_id, denylist=DENYLIST, source=R["bronze"],
        events_table=R["events"], stock_table=R["stock"], log_table=R["log"], **switches,
    )


run_r(R["app_id"])  # life 1: fresh checkpoint, empty log — the guard (on) lets it through
life1 = lines(R)
print(f"life 1: {[r.asDict() for r in life1]}\nstock {stock_totals(R)}")
assert len(life1) == 1 and life1[0].bronze_rows == 158_625
assert stock_totals(R) == (34_448, 993_982)

spark.sql(f"INSERT INTO {R['bronze']} SELECT * FROM {BRONZE_CDC} WHERE op = 'D'")
assert spark.table(R["bronze"]).count() == 158_725

# COMMAND ----------

# MAGIC %md
# MAGIC ### R1 — checkpoint deleted, same app id. **Bug first.**

# COMMAND ----------

forget_checkpoint(R["checkpoint"])
assert not checkpoint_has_history(R["checkpoint"])
events_merges_before = len(commits(R["events"], "MERGE"))
run_r(R["app_id"], guard_reset=False)

after_r1 = lines(R)
merges = commits(R["events"], "MERGE")
life2_events = [
    int(m.operationMetrics.get("numTargetRowsInserted", 0)) for m in merges[events_merges_before:]
]
extra, missing = compare_stock(R["stock"], expected_stock(R["events"]))
logged = sum(r.bronze_rows for r in after_r1)
bronze_now = spark.table(R["bronze"]).count()
alarm = lag_alarm(R["bronze"], R["log"])
print(f"life 2 event-log MERGEs inserted {life2_events}; stock {stock_totals(R)}; vs the log extra={extra} "
      f"missing={missing}")
print(f"CDC log: {len(after_r1)} line(s), {logged:,} Bronze rows logged; Bronze holds {bronze_now:,}; "
      f"log WRITE commits {len(commits(R['log'], 'WRITE'))}; lag alarm on these tables: {alarm}")

# The data is right: the 100 delists were applied ...
assert sum(life2_events) == 100
assert stock_totals(R) == (34_348, 991_530) and (extra, missing) == (0, 0)
# ... and the log never says so: Delta skipped life 2's line (batch 0 <= the 0 it remembers).
assert len(after_r1) == 1 and logged == 158_625 and bronze_now - logged == 100
assert len(commits(R["log"], "WRITE")) == 1
# The lag alarm counts those 100 rows as missing from Silver, which has them: a false alarm.
assert alarm == 1
print("BUG PRESENT: the delists were applied, the log was silently not written, and the lag alarm fires "
      "for rows Silver already holds")

# COMMAND ----------

# MAGIC %md
# MAGIC ### R2 — someone "fixes" the short log with a new app id (checkpoint deleted again)

# COMMAND ----------

forget_checkpoint(R["checkpoint"])
run_r(R["app_id"] + ".v2", guard_reset=False)
after_r2 = lines(R)
logged = sum(r.bronze_rows for r in after_r2)
print(f"CDC log: {[(r.batch_id, r.bronze_rows, r.inserted, r.updated, r.deleted) for r in after_r2]}; "
      f"{logged:,} Bronze rows logged for {spark.table(R['bronze']).count():,} in Bronze; "
      f"stock {stock_totals(R)}")
assert len(after_r2) == 2 and logged == 158_625 + 158_725
assert (after_r2[1].inserted, after_r2[1].updated, after_r2[1].deleted) == (0, 0, 0)
assert stock_totals(R) == (34_348, 991_530)

# 100 more rows land in Bronze, and Silver never runs. The alarm compares counts, and the log now counts
# 158,625 rows too many.
spark.sql(f"INSERT INTO {R['bronze']} SELECT * FROM {BRONZE_CDC} WHERE op = 'D'")
waiting = spark.table(R["bronze"]).count() - 158_725
alarm = lag_alarm(R["bronze"], R["log"])
print(f"{waiting} rows waiting, never applied; lag alarm: {alarm}")
assert waiting == 100 and alarm == 0
print("BUG PRESENT: every row logged twice, and the lag alarm is blind until Bronze grows by 158,625 rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## G — the guard: a stream with no history may only write into an empty log
# MAGIC
# MAGIC Bronze's `refuse_unsafe_reset`, applied to the log only: the drill above shows the data tables are
# MAGIC safe to replay into, so they are not required empty (a Silver orders rebuild from Bronze version 0
# MAGIC stops being possible around 2026-10-27; a guard that demanded empty data tables would force one).

# COMMAND ----------

for app_id in (R["app_id"], R["app_id"] + ".v3"):
    forget_checkpoint(R["checkpoint"])
    before = len(lines(R))
    try:
        run_r(app_id)
        refused = None
    except UnsafeStreamReset as e:
        refused = str(e)
    print(f"app id {app_id}: {(refused or 'NOT refused')[:200]}")
    assert refused, "the guard let a reset through"
    assert len(lines(R)) == before and not checkpoint_has_history(R["checkpoint"]), "something ran"

for cp in PRODUCTION_CHECKPOINTS:
    print(f"{cp}: history {checkpoint_has_history(cp)}")
    assert checkpoint_has_history(cp), "the daily run would be refused"
print("GUARD SEEN: both resets refused before any stream started; production's checkpoints pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B — Bronze's history applied backwards
# MAGIC
# MAGIC A CDC feed's order is everything, and Silver claims two defences: the `seq` guard, and (since
# MAGIC Session 10) each SKU's newest event taken from the whole log. Production read Bronze in order (one
# MAGIC batch, then the delists). Here every Bronze batch is fed to the batch function **newest first** — the
# MAGIC worst order a replay could produce.
# MAGIC
# MAGIC **Prediction, from Bronze alone** (no MERGE): walking the batches newest first, a SKU is inserted by
# MAGIC the first batch holding it and updated by each later batch that brings a higher `seq` than any seen
# MAGIC (only re-sent copies can); a SKU whose newest event is a delete is never inserted: its `D` is first.

# COMMAND ----------

batch_ids = spark.table(BRONZE_CDC).select("_batch_id").distinct().orderBy("_batch_id")
batches = [r._batch_id for r in batch_ids.collect()]
backwards = list(reversed(batches))
print(f"Bronze batches {batches}; applied as {backwards}")

kept = spark.table(BRONZE_CDC).where(~F.col("event_id").isin(DENYLIST))
last_op = kept.groupBy("sku_key").agg(F.max_by("op", "seq").alias("last_op"))
newest_is_d = last_op.where("last_op = 'D'").select("sku_key")
per_batch = kept.groupBy("sku_key", "_batch_id").agg(F.max("seq").alias("top"))
order = Window.partitionBy("sku_key").orderBy(F.col("_batch_id").desc()).rowsBetween(
    Window.unboundedPreceding, -1
)
walk = per_batch.join(newest_is_d, "sku_key", "left_anti").withColumn("_seen", F.max("top").over(order))
predicted = {
    r._batch_id: (r.ins, r.upd)
    for r in walk.groupBy("_batch_id").agg(
        F.count_if(F.col("_seen").isNull()).alias("ins"),
        F.count_if(F.col("_seen").isNotNull() & (F.col("top") > F.col("_seen"))).alias("upd"),
    ).collect()
}
print(f"predicted (inserted, updated) per Bronze batch: {predicted}")


def apply_backwards(t, **switches):
    create_tables(t["events"], t["stock"], t["log"])
    apply_batch = make_batch_writer(
        denylist=DENYLIST, app_id=t["app_id"], events_table=t["events"], stock_table=t["stock"],
        log_table=t["log"], **switches,
    )
    for k, b in enumerate(backwards):
        apply_batch(spark.table(BRONZE_CDC).where(F.col("_batch_id") == b), k)
    return apply_batch

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1 — production code

# COMMAND ----------

B1 = scratch("b1")
apply_b1 = apply_backwards(B1)
got = lines(B1)
for k, r in enumerate(got):
    print(f"applied {k} <- Bronze batch {backwards[k]}: rows {r.bronze_rows:,} events {r.events:,} "
          f"+{r.inserted:,} ~{r.updated:,} -{r.deleted:,} chain breaks {r.chain_breaks:,}  "
          f"predicted {predicted.get(backwards[k], (0, 0))}")

prod_stock = spark.table(STOCK).select(*STOCK_COLUMNS)
mine = spark.table(B1["stock"]).select(*STOCK_COLUMNS)
vs_production = (mine.exceptAll(prod_stock).count(), prod_stock.exceptAll(mine).count())
# produced_at is the wall clock at send time, so a re-sent copy carries its own; which copy is kept is
# checked below, with the Kafka coordinates.
FACTS = [c for c in EVENT_COLUMNS if c != "produced_at"]
prod_events = spark.table(EVENTS).select(*FACTS)
my_events = spark.table(B1["events"]).select(*FACTS)
events_vs_production = (my_events.exceptAll(prod_events).count(), prod_events.exceptAll(my_events).count())
report = chain_report(B1["events"])
per_batch_breaks = sum(r.chain_breaks for r in got)
print(f"stock {stock_totals(B1)} vs production {vs_production}; "
      f"event log vs production {events_vs_production}")
print(f"chain, whole log: {report.asDict()}; summed per batch: {per_batch_breaks}")

assert all((r.inserted, r.updated) == predicted.get(backwards[k], (0, 0)) for k, r in enumerate(got))
assert sum(r.deleted for r in got) == 0, "a delete met a row: the delists arrived first, so none should"
assert vs_production == (0, 0) and events_vs_production == (0, 0)
assert (report.breaks, report.broken_skus, report.off_skus, report.off_units, report.units_sold) == (
    1_423, 237, 26, 27, 112_650
)
# The per-batch count checks each event against the one before it IN THE LOG SO FAR. Backwards, a SKU's
# earlier events are not there yet, so links across batches are never checked.
assert per_batch_breaks < 1_423
print(f"HELD: backwards, Silver ends identical to production. NOT HELD: the per-batch chain count "
      f"({per_batch_breaks}) — exact only when events arrive in order; the whole-log report is the truth")

# Lineage columns record WHICH copy was kept: backwards, a re-sent copy in a later batch is applied first.
copies_span = kept.groupBy("event_id").agg(F.count_distinct("_batch_id").alias("n")).where("n > 1").count()
coords = ["event_id", "produced_at", *COORDINATES]
moved = spark.table(B1["events"]).select(*coords).exceptAll(spark.table(EVENTS).select(*coords)).count()
print(f"events with copies in more than one Bronze batch: {copies_span}; logged from another copy: {moved}")
assert moved <= copies_span, "a copy changed for an event that only ever had one"

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2 — Session 9's code (newest event from the batch), backwards. **Bug first** — at scale this time
# MAGIC
# MAGIC exp_04 C showed one deleted SKU coming back on a hand-made example. Backwards, every delisted SKU's
# MAGIC `D` arrives first and finds nothing to delete; each older batch then brings an update that matches
# MAGIC nothing — and is inserted.

# COMMAND ----------

B2 = scratch("b2")
apply_backwards(B2, newest_from="batch")
extra, missing = compare_stock(B2["stock"], expected_stock(B2["events"]))
delisted = {r.sku_key for r in newest_is_d.collect()}
back = {r.sku_key for r in spark.table(B2["stock"]).where(F.col("sku_key").isin(list(delisted))).collect()}
print(f"newest from the batch: stock {stock_totals(B2)}; vs the log extra={extra} missing={missing}; "
      f"delisted SKUs back in stock: {len(back)}")
assert len(delisted) == 100 and back == delisted
assert (extra, missing) == (100, 0)
print("BUG PRESENT (Session 9's code): all 100 delisted SKUs resurrected. Production's code: none (B1)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### B3 — the last batch again, same batch id (a replay after a crash)

# COMMAND ----------

before = (len(lines(B1)), spark.table(B1["events"]).count(), stock_totals(B1))
merges_before = {t: len(commits(B1[t], "MERGE")) for t in ("events", "stock")}
last = len(backwards) - 1
apply_b1(spark.table(BRONZE_CDC).where(F.col("_batch_id") == backwards[last]), last)
after = (len(lines(B1)), spark.table(B1["events"]).count(), stock_totals(B1))
METRICS = ("numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted")
replay = {
    t: [{k: m.operationMetrics.get(k) for k in METRICS} for m in commits(B1[t], "MERGE")[merges_before[t]:]]
    for t in ("events", "stock")
}
print(f"before {before}\nafter  {after}\nthe replay's MERGE commits: {replay}")
assert before == after
assert all(int(v or 0) == 0 for ms in replay.values() for m in ms for v in m.values())
print("HELD: a replayed batch changes nothing; its log line is skipped by txnVersion")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C — incident regression: a new broken link in the before/after chain (incidents.md 2026-10-01)
# MAGIC
# MAGIC The feed's 1,423 known breaks are counted, not repaired; the guard is that **a new one shows**. In
# MAGIC production that is Verify 3's pinned 1,423, which fails the daily Job. Here: one new event for a SKU
# MAGIC with no break, whose "stock before" is 7 units off the stock Silver holds.

# COMMAND ----------

clean_sku = spark.sql(
    f"""
    WITH o AS (SELECT sku_key, seq, op, prev_stock_qty, stock_qty,
                      lag(stock_qty) OVER (PARTITION BY sku_key ORDER BY seq) AS before FROM {B1['events']})
    SELECT sku_key FROM o GROUP BY sku_key
    HAVING sum(CASE WHEN before IS NOT NULL AND NOT (prev_stock_qty <=> before) THEN 1 ELSE 0 END) = 0
       AND max_by(op, seq) <> 'D'
    ORDER BY sku_key LIMIT 1
    """
).first().sku_key
held_now = spark.table(B1["stock"]).where(F.col("sku_key") == clean_sku).first()
newest = spark.table(B1["events"]).where(F.col("sku_key") == clean_sku).orderBy(F.col("seq").desc()).limit(1)
broken = newest.select(
    F.sha2(F.concat(F.col("event_id"), F.lit("-drill2-broken")), 256).substr(1, 32).alias("event_id"),
    "event_ts", "produced_at", F.lit("U").alias("op"), (F.col("seq") + 1).alias("seq"),
    "sku_key", "product_id", "seller_id",
    (F.col("stock_qty") + 6).cast("int").alias("stock_qty"),
    (F.col("stock_qty") + 7).cast("int").alias("prev_stock_qty"),
    F.lit(None).cast("int").alias("_kafka_partition"), F.lit(None).cast("bigint").alias("_kafka_offset"),
)
apply_b1(broken, len(backwards))
line = lines(B1)[-1]
print(f"SKU {clean_sku} held {held_now.stock_qty}; new event says before {held_now.stock_qty + 7}, "
      f"after {held_now.stock_qty + 6}\nits batch: {line.asDict()}\n"
      f"whole log: {chain_report(B1['events']).breaks}")
assert (line.events_inserted, line.updated, line.chain_breaks) == (1, 1, 1)
assert chain_report(B1["events"]).breaks == 1_424
print("GUARD SEEN: the new break is counted; in production the pinned 1,423 would fail the Job")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look at what happened

# COMMAND ----------

HISTORY_COLUMNS = ["version", "timestamp", "operation", "operationMetrics"]
display(spark.sql(f"DESCRIBE HISTORY {R['log']}").select(*HISTORY_COLUMNS))

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {R['events']}").select(*HISTORY_COLUMNS))