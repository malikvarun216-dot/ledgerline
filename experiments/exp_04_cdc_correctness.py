# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # exp_04 — CDC correctness: remove each guard of Silver inventory and watch it break
# MAGIC
# MAGIC Silver inventory (Session 9) keeps the current stock per SKU from a change feed. Four things keep it
# MAGIC right, and each part below takes one away, with the **production code** (`_merge_inventory_cdc`)
# MAGIC and its switches, on scratch tables (`workspace.silver.exp04_*`, rebuilt every run):
# MAGIC
# MAGIC | Part | Guard removed | Expected |
# MAGIC |---|---|---|
# MAGIC | A1 | in-batch dedup; two changes to a SKU **in Silver** | MERGE refused; retry with dedup applies |
# MAGIC | A2 | in-batch dedup; two changes to a **new** SKU | no error, SKU **inserted twice** (B25) |
# MAGIC | B | `seq` guard; an older change arriving late | older overwrites newer; guarded: ignored |
# MAGIC | C | none — a design gap: an update after a newer **delete** | SKU comes back; fix: `newest_from` |
# MAGIC | D | tie refusal, on Bronze's **real** contamination | refused → denylist; guard off → `RESTORE` |
# MAGIC
# MAGIC Real events throughout: from `silver.inventory_events` (the clean log) and, for D, the contaminated
# MAGIC rows still in `bronze.inventory_cdc`. Both read-only. Contaminating the live topic was rejected
# MAGIC (decisions.md, Session 10). Each part asserts the bug **present** before the fix.
# MAGIC
# MAGIC **Steps that must fail do not run a stream.** A stream that dies inside a notebook command fails the
# MAGIC command even when the code catches the error, and "Run all" stops there (incidents.md, 2026-09-27,
# MAGIC exp_01 — repeated by this notebook's first version, 2026-10-02). So a failing step calls the
# MAGIC stream's own batch function on exactly the rows its next micro-batch would read, with the batch id
# MAGIC it would use. The checkpoint does not move, so the next real run reads those rows again — the same
# MAGIC retry a failed micro-batch gets.

# COMMAND ----------

# MAGIC %run ../databricks/silver/_merge_inventory_cdc

# COMMAND ----------

import uuid

# Checkpoints are never deleted in place: every run of this notebook gets new ones.
RUN = uuid.uuid4().hex[:8]
SOURCE_COLUMNS = [*EVENT_COLUMNS, *COORDINATES]
DENYLIST = load_denylist()
assert len(DENYLIST) == 53
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.silver.checkpoints")


def scratch(part):
    """Empty scratch Bronze, Silver tables and a new checkpoint for one part."""
    t = {k: f"{SILVER}.exp04_{part}_{k}" for k in ("bronze", "events", "stock", "log")}
    for name in t.values():
        spark.sql(f"DROP TABLE IF EXISTS {name}")
    spark.table(EVENTS).select(*SOURCE_COLUMNS).limit(0).write.saveAsTable(t["bronze"])
    t["checkpoint"] = f"{CHECKPOINTS}/exp04/{RUN}/{part}"
    t["app_id"] = f"ledgerline.exp04.{RUN}.{part}"
    return t


def land(t, rows):
    """Append to the scratch Bronze. One append = one micro-batch, because each step reads one append."""
    schema = spark.table(t["bronze"]).schema
    typed = rows.select(*[F.col(f.name).cast(f.dataType) for f in schema])
    typed.write.mode("append").saveAsTable(t["bronze"])


def run(t, denylist=(), **switches):
    """One AvailableNow pass with the production batch function. ONLY for steps expected to succeed."""
    run_silver_cdc(
        checkpoint=t["checkpoint"], app_id=t["app_id"], denylist=list(denylist), source=t["bronze"],
        events_table=t["events"], stock_table=t["stock"], log_table=t["log"], **switches,
    )


def pending(t):
    """The rows of the latest append to the scratch Bronze: what the stream's next micro-batch reads.
    Found by operation, not "latest version": Delta may add an OPTIMIZE commit of its own (Session 8)."""
    writes = spark.sql(f"DESCRIBE HISTORY {t['bronze']}").where("operation = 'WRITE'")
    v = writes.agg(F.max("version")).first()[0]
    now = spark.read.option("versionAsOf", v).table(t["bronze"])
    return now.exceptAll(spark.read.option("versionAsOf", v - 1).table(t["bronze"]))


def apply_directly(t, denylist=(), **switches):
    """The stream's next micro-batch without the stream, for steps expected to FAIL: the production batch
    function on `pending(t)`, with the batch id the stream would use. None, or the error's text."""
    create_tables(t["events"], t["stock"], t["log"])
    apply_batch = make_batch_writer(
        denylist=list(denylist), app_id=t["app_id"], events_table=t["events"], stock_table=t["stock"],
        log_table=t["log"], **switches,
    )
    try:
        apply_batch(pending(t), len(lines(t)))
    except Exception as e:
        return str(e)
    return None


def stock(t):
    return {r.sku_key: (r.seq, r.stock_qty) for r in spark.table(t["stock"]).collect()}


def lines(t):
    return spark.table(t["log"]).orderBy("batch_id").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real SKUs to work with
# MAGIC
# MAGIC `A` and `N`: at least 4 events, never deleted, and four different stock levels in their first four
# MAGIC events (so a wrong one is visible). `X`: one of Session 9's 100 delisted SKUs, at least 3 changes
# MAGIC before its delete.

# COMMAND ----------

by_seq = Window.partitionBy("sku_key").orderBy("seq")
ranked = spark.table(EVENTS).withColumn("_i", F.row_number().over(by_seq))
per_sku = spark.table(EVENTS).groupBy("sku_key").agg(
    F.count(F.lit(1)).alias("n"), F.count_if(F.col("op") == "D").alias("deletes")
)
first4 = ranked.where("_i <= 4").groupBy("sku_key").agg(F.count_distinct("stock_qty").alias("levels"))
A, N = [
    r.sku_key for r in per_sku.join(first4, "sku_key").where("n >= 4 AND deletes = 0 AND levels = 4")
    .orderBy("sku_key").limit(2).collect()
]
X = per_sku.where("n >= 4 AND deletes = 1").orderBy("sku_key").first().sku_key
H = {sku: ranked.where(F.col("sku_key") == sku).orderBy("_i").collect() for sku in (A, N, X)}


def pick(sku, *nth):
    """A SKU's events by position in its own seq order (1 = first)."""
    return ranked.where((F.col("sku_key") == sku) & F.col("_i").isin(*nth)).drop("_i")


def at(sku, nth):
    r = H[sku][nth - 1]
    return (r.seq, r.stock_qty)


for sku in (A, N, X):
    first = [(r.op, r.seq, r.prev_stock_qty, r.stock_qty) for r in H[sku][:5]]
    print(sku, first, f"... {len(H[sku])} events")
assert H[X][-1].op == "D"

# COMMAND ----------

# MAGIC %md
# MAGIC ## A1 — no in-batch dedup, two changes to a SKU already in Silver
# MAGIC
# MAGIC Batch 0: A's first event. Batch 1: A's 2nd and 3rd, and N's 1st and 2nd. Without dedup, two source
# MAGIC rows match A's one target row and Delta cannot know which should win. **Bug first.**

# COMMAND ----------

a1 = scratch("a1")
land(a1, pick(A, 1))
run(a1)
land(a1, pick(A, 2, 3).unionByName(pick(N, 1, 2)))
err = apply_directly(a1, dedup=False)
print((err or "no error")[:400])
assert err and ("MULTIPLE_SOURCE_ROW" in err or "multiple source rows matched" in err.lower())

logged = spark.table(a1["events"]).count()
print(f"after the refused MERGE: stock {stock(a1)}, event log {logged} rows, CDC log {len(lines(a1))} line")
assert stock(a1) == {A: at(A, 1)}, "the stock changed although the MERGE failed"
assert logged == 5, "expected the event-log MERGE (which runs first) to have committed the batch's 4 events"
assert len(lines(a1)) == 1

# COMMAND ----------

# MAGIC %md
# MAGIC **The fix, as production does it:** the same checkpoint, dedup on. The stream reads batch 1 — the
# MAGIC same Bronze rows the failed attempt had — and this time it applies. The event log adds nothing (it
# MAGIC already has them).

# COMMAND ----------

run(a1)
retried = lines(a1)[1]
print(f"stock {stock(a1)}\nbatch 1 line: {retried.asDict()}")
assert stock(a1) == {A: at(A, 3), N: at(N, 2)}
assert (retried.events_inserted, retried.updated, retried.inserted) == (0, 1, 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A2 — no in-batch dedup, two changes to a SKU Silver has never seen
# MAGIC
# MAGIC Both rows match nothing, so both go to `WHEN NOT MATCHED … INSERT`. Delta's "multiple source rows"
# MAGIC check is about rows that *match*; nothing checks two inserts of one key. Expectation (B25, never
# MAGIC verified): no error, and the SKU twice.

# COMMAND ----------

a2 = scratch("a2")
land(a2, pick(N, 1, 2))
err = apply_directly(a2, dedup=False)
rows_n = spark.table(a2["stock"]).where(F.col("sku_key") == N).count()
print(f"error: {(err or 'none')[:300]}\nrows for N: {rows_n}")
assert err is None and rows_n == 2, "B25's expectation was wrong — record what Delta did instead"

# COMMAND ----------

# MAGIC %md
# MAGIC ## B — no `seq` guard, an older change arriving after a newer one
# MAGIC
# MAGIC Batch 0: A's 1st and 3rd events (dedup keeps the 3rd). Batch 1: A's 2nd, late. **Bug first**, then the
# MAGIC guard, then a newer change — the UPDATE path and the "older event ignored" path, which live data has
# MAGIC never exercised (Session 9: one batch of inserts, one of deletes).

# COMMAND ----------

b1 = scratch("b1")
land(b1, pick(A, 1, 3))
run(b1)
assert stock(b1) == {A: at(A, 3)}
land(b1, pick(A, 2))
run(b1, seq_guard=False)
print(f"no guard: A is {stock(b1)[A]}, the 3rd event said {at(A, 3)}, the late 2nd said {at(A, 2)}")
assert stock(b1) == {A: at(A, 2)}, "expected the older event to overwrite the newer stock"

b2 = scratch("b2")
land(b2, pick(A, 1, 3))
run(b2)
land(b2, pick(A, 2))
run(b2)
late = lines(b2)[1]
print(f"guard on, late event: A is {stock(b2)[A]}; line {late.asDict()}")
assert stock(b2) == {A: at(A, 3)}
assert (late.events_inserted, late.updated, late.inserted, late.deleted) == (1, 0, 0, 0)
land(b2, pick(A, 4))
run(b2)
newer = lines(b2)[2]
print(f"newer event: A is {stock(b2)[A]}; line {newer.asDict()}")
assert stock(b2) == {A: at(A, 4)}
assert (newer.updated, newer.inserted) == (1, 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C — an update arriving after a newer delete brings the SKU back
# MAGIC
# MAGIC Batch 0: X's 1st and 2nd events. Batch 1: X's delete. Batch 2: X's 3rd event, late — older than the
# MAGIC delete. The delete removed X's row and its `seq` with it, so the late update matches nothing and is
# MAGIC inserted. Session 9 named this gap and bound it here. **Bug first**, with Session 9's code
# MAGIC (`newest_from="batch"`); production reads the newest event from the log since Session 10.

# COMMAND ----------

K = len(H[X])


def delete_then_late_update(part, **switches):
    t = scratch(part)
    land(t, pick(X, 1, 2))
    run(t, **switches)
    land(t, pick(X, K))
    run(t, **switches)
    assert stock(t) == {}, "the delete did not delete"
    land(t, pick(X, 3))
    run(t, **switches)
    return t


c1 = delete_then_late_update("c1", newest_from="batch")
extra, missing = compare_stock(c1["stock"], expected_stock(c1["events"]))
print(f"newest from the batch: stock {stock(c1)}; vs the log's newest events extra={extra} missing={missing}")
assert stock(c1) == {X: at(X, 3)}, "expected the deleted SKU to come back"
assert (extra, missing) == (1, 0), "Session 9's Verify 2 should see the resurrected row"

c2 = delete_then_late_update("c2", newest_from="log")
extra, missing = compare_stock(c2["stock"], expected_stock(c2["events"]))
print(f"newest from the log: stock {stock(c2)}; extra={extra} missing={missing}")
print(f"its late batch: {lines(c2)[2].asDict()}")
assert stock(c2) == {} and (extra, missing) == (0, 0)

# COMMAND ----------

# MAGIC %md
# MAGIC **Does it happen on the live feed?** Every event in Bronze, one copy each, in arrival order per SKU
# MAGIC (one SKU is one Kafka partition, so the offset is the arrival order): how many arrive with a `seq`
# MAGIC no higher than one already seen for their SKU? Read-only. The 2026-09-26 contamination was sent after
# MAGIC the clean run, with early `seq`s, so it should be exactly those 53; the clean feed, none.

# COMMAND ----------

first_copy = Window.partitionBy("event_id").orderBy(*COORDINATES)
arrived = (
    spark.table(BRONZE_CDC)
    .withColumn("_copy", F.row_number().over(first_copy))
    .where("_copy = 1")
)
earlier = Window.partitionBy("sku_key").orderBy(*COORDINATES).rowsBetween(Window.unboundedPreceding, -1)
late = arrived.withColumn("_max_before", F.max("seq").over(earlier)).where("seq <= _max_before")
late_all = late.count()
late_clean = late.where(~F.col("event_id").isin(DENYLIST)).count()
print(f"events arriving behind their SKU's order: {late_all}; outside the denylist: {late_clean}")
assert (late_all, late_clean) == (53, 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D — the tie refusal, on Bronze's real contamination
# MAGIC
# MAGIC Batch 0: the clean events of the 23 SKUs the contamination touched. Batch 1: the 159 contaminated
# MAGIC Bronze rows (53 events, sent three times by the 2026-09-26 test run), each on exactly the `seq` of a
# MAGIC clean event.
# MAGIC
# MAGIC 1. **Guard on, no denylist** → batch 1 refused, nothing written.
# MAGIC 2. **The person's fix: the denylist** → the same batch retried, 159 rows dropped, Silver unchanged.
# MAGIC 3. **Guard off** (fresh tables) → the 53 bad events reach the event log; units sold goes wrong.
# MAGIC 4. **Repair by time travel**: look at the log as it was, `RESTORE` it, compare with step 2.

# COMMAND ----------

bad = spark.table(BRONZE_CDC).where(F.col("event_id").isin(DENYLIST)).select(*SOURCE_COLUMNS)
bad_skus = [r.sku_key for r in bad.select("sku_key").distinct().collect()]
clean = spark.table(EVENTS).where(F.col("sku_key").isin(bad_skus)).select(*SOURCE_COLUMNS)
n_bad, n_clean = bad.count(), clean.count()
print(f"contaminated rows {n_bad} on {len(bad_skus)} SKUs; clean events of those SKUs {n_clean}")
assert (n_bad, len(bad_skus)) == (159, 23)


def units_sold(events_table):
    return spark.table(events_table).where("prev_stock_qty > stock_qty").agg(
        F.sum(F.col("prev_stock_qty") - F.col("stock_qty")).alias("u")
    ).first().u or 0


d = scratch("d1")
land(d, clean)
run(d)
land(d, bad)
err = apply_directly(d)
print(f"1. guard on, no denylist:\n{(err or 'no error')[:400]}")
assert err and ("BatchRefused" in err or "Nothing was written" in err)
assert spark.table(d["events"]).count() == n_clean and len(lines(d)) == 1

run(d, denylist=DENYLIST)
fixed = lines(d)[1]
print(f"2. with the denylist: {fixed.asDict()}")
assert (fixed.bronze_rows, fixed.denied_rows, fixed.events, fixed.events_inserted) == (159, 159, 0, 0)
assert (fixed.inserted, fixed.updated, fixed.deleted) == (0, 0, 0)
assert spark.table(d["events"]).count() == n_clean

# COMMAND ----------

p = scratch("d3")
land(p, clean)
run(p)
land(p, bad)
run(p, refuse=False)
polluted = lines(p)[1]
print(f"3. guard off: {polluted.asDict()}")
print(f"   event log {spark.table(p['events']).count()} rows (clean {n_clean}); units sold "
      f"{units_sold(p['events'])} vs {units_sold(d['events'])}")
assert spark.table(p["events"]).count() == n_clean + 53
assert units_sold(p["events"]) != units_sold(d["events"])
same_stock = spark.table(p["stock"]).select(*STOCK_COLUMNS).exceptAll(
    spark.table(d["stock"]).select(*STOCK_COLUMNS)
).count()
print(f"   stock rows that differ from the guarded run: {same_stock} "
      "(Session 6: the bad events carry early seqs, so the guard ignores them for the stock)")
assert same_stock == 0

# COMMAND ----------

# MAGIC %md
# MAGIC **4. Repair by time travel.** The CDC log line for the bad batch says which version of the event log
# MAGIC it wrote (`events_version`) — find the commit by what it did, never "the latest version" (Session 8).
# MAGIC First *read* the log as it was one version earlier, then `RESTORE` to it.

# COMMAND ----------

good_version = polluted.events_version - 1
as_it_was = spark.read.option("versionAsOf", good_version).table(p["events"]).count()
print(f"event log at version {good_version}: {as_it_was} rows")
assert as_it_was == n_clean

display(spark.sql(f"RESTORE TABLE {p['events']} TO VERSION AS OF {good_version}"))
restored = spark.table(p["events"]).select(*SOURCE_COLUMNS)
guarded = spark.table(d["events"]).select(*SOURCE_COLUMNS)
diff = (restored.exceptAll(guarded).count(), guarded.exceptAll(restored).count())
print(f"restored vs the guarded run: {diff}; units sold {units_sold(p['events'])}")
assert diff == (0, 0)
assert units_sold(p["events"]) == units_sold(d["events"])

history = spark.sql(f"DESCRIBE HISTORY {p['events']}").where("operation = 'RESTORE'").first()
print(f"RESTORE commit v{history.version}: {history.operationParameters} {history.operationMetrics}")

# COMMAND ----------

# MAGIC %md
# MAGIC **What a RESTORE looks like to the change feed** — Gold will read Silver through the change feed
# MAGIC (Session 13). If the RESTORE emits no `delete` rows for the 53 events it removed, Gold keeps them.
# MAGIC Recorded, not assumed.

# COMMAND ----------

try:
    feed = spark.sql(
        f"SELECT _change_type, count(*) AS n "
        f"FROM table_changes('{p['events']}', {history.version}) GROUP BY 1"
    ).collect()
    print(f"change feed at the RESTORE version: {[(r._change_type, r.n) for r in feed]}")
except Exception as e:
    print(f"change feed at the RESTORE version: error — {str(e)[:400]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — what each scratch table's history shows

# COMMAND ----------

for t in (a1, b2, c1, d, p):
    print(t["log"])
    display(spark.table(t["log"]).orderBy("batch_id"))