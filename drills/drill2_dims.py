# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Drill 2 — Silver dims attacked: a rebuild, a lost merge log, an old night corrected
# MAGIC
# MAGIC Silver dims is not a stream: each night is one `MERGE … WHEN NOT MATCHED BY SOURCE DELETE`, and the
# MAGIC **merge log** (`silver.dims_merge_log`) says which night Silver shows. That log is this path's
# MAGIC checkpoint. The production code (`_merge_dims.apply_dim`) on two scratch schemas,
# MAGIC `workspace.drill2_bronze` (copies of Bronze dims) and `workspace.drill2_silver`; production is only
# MAGIC read.
# MAGIC
# MAGIC | Part | Attack | Expected |
# MAGIC |---|---|---|
# MAGIC | S | rebuild all three dims from empty | the merge log and every column = production |
# MAGIC | L1 | the merge log lost, Silver kept | customer: refused by the breaker; seller: replayed (below) |
# MAGIC | L2 | the guard | both refused before any MERGE; production passes |
# MAGIC | L3 | the production repair: `RESTORE` the log | "nothing new", Silver untouched |
# MAGIC | O | an **old** night re-delivered, corrected | not applied — and is anyone told? |

# COMMAND ----------

# MAGIC %run ../databricks/silver/_merge_dims

# COMMAND ----------

import os

D_BRONZE, D_SILVER = "workspace.drill2_bronze", "workspace.drill2_silver"
D_LOG, D_REJECTED = f"{D_SILVER}.dims_merge_log", f"{D_SILVER}.dims_rejected_rows"
LINEAGE_CHECKED = [c for c in LINEAGE if c != "_merged_at"]


def run(dim, **switches):
    return apply_dim(dim, bronze=D_BRONZE, silver=D_SILVER, log=D_LOG, rejected=D_REJECTED, **switches)


def columns(dim):
    return [*business_columns(dim), *LINEAGE_CHECKED]


def vs_production(dim):
    mine = spark.table(f"{D_SILVER}.{dim}").select(*columns(dim))
    prod = spark.table(f"{SILVER}.{dim}").select(*columns(dim))
    return mine.exceptAll(prod).count(), prod.exceptAll(mine).count()


def log_lines(dim, log=D_LOG):
    return [
        (str(r.dump_date), (r.inserted, r.updated, r.deleted))
        for r in spark.table(log).where(F.col("dimension") == dim).orderBy("applied_at").collect()
    ]


def repo_file(*parts):
    here = os.getcwd()
    for _ in range(4):
        path = os.path.join(here, *parts)
        if os.path.exists(path):
            with open(path) as fh:
                return fh.read()
        here = os.path.dirname(here)
    raise FileNotFoundError(os.path.join(*parts))


ALERT_SQL = repo_file("databricks", "alerts", "silver_behind_bronze.sql")


def lag_alarm():
    """The production alert query with the dims tables swapped for the scratch ones (CDC and orders still
    read production: 0). One row: `sources_behind` and the part that fired."""
    sql = ALERT_SQL.replace("workspace.silver.dims_merge_log", D_LOG)
    for dim in DIMS:
        sql = sql.replace(f"workspace.bronze.{dim}", f"{D_BRONZE}.{dim}")
    return spark.sql(sql).first()


production = spark.sql(ALERT_SQL).first()
print(f"the lag alarm on production today: {production.asDict()}")
assert production.sources_behind == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## S — rebuild from empty: the only reset this path allows
# MAGIC
# MAGIC Each dimension from an empty table and an empty log (the guard lets that through). Expected: every
# MAGIC night's `(inserted, updated, deleted)` equal to the first time production applied it, and every
# MAGIC column of every row equal to production (lineage included, `_merged_at` aside).

# COMMAND ----------

for schema in (D_BRONZE, D_SILVER):
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
for dim in DIMS:
    spark.sql(f"CREATE TABLE {D_BRONZE}.{dim} AS SELECT * FROM {BRONZE}.{dim}")
create_merge_log(D_LOG)
for dim in DIMS:
    run(dim)

first_applied = {
    (r.dimension, r.night): (r.inserted, r.updated, r.deleted)
    for r in spark.sql(
        f"""SELECT dimension, date_format(dump_date, 'yyyy-MM-dd') AS night,
                   min_by(inserted, applied_at) AS inserted, min_by(updated, applied_at) AS updated,
                   min_by(deleted, applied_at) AS deleted
            FROM {MERGE_LOG} GROUP BY 1, 2"""
    ).collect()
}
for dim in DIMS:
    mine = log_lines(dim)
    diff = vs_production(dim)
    print(f"{dim:9} rebuilt: {mine}\n          vs production, every column: {diff}")
    assert mine == sorted((n, v) for (d, n), v in first_applied.items() if d == dim)
    assert diff == (0, 0)
print(f"lag alarm on the rebuilt tables: {lag_alarm().asDict()}")
assert lag_alarm().sources_behind == 0
LOG_COMPLETE = spark.sql(f"DESCRIBE HISTORY {D_LOG} LIMIT 1").first().version

# COMMAND ----------

# MAGIC %md
# MAGIC ## L1 — the merge log is lost; Silver is not. **Bug first.**
# MAGIC
# MAGIC `plan_nights` reads the log to know where Silver is. With no line for a dimension it starts from the
# MAGIC first night and MERGEs each one over today's table. **Prediction:**
# MAGIC - **customer** — night 2016-09-04 holds 1 customer; the delete clause would remove every other row →
# MAGIC   the 5% breaker refuses it. Silver untouched, the job fails (an email).
# MAGIC - **seller** — night 2016-09-04 holds all 3,095 sellers, so nothing is deleted and the breaker sees
# MAGIC   nothing wrong: the 100 sellers deleted on 2017-12-28 are **inserted again**, every changed seller is
# MAGIC   put back to its first value, then nights 2 to 5 replay. Silver ends right — and the change feed,
# MAGIC   which Gold will read (S13), carries the whole history a second time.

# COMMAND ----------

spark.sql(f"DELETE FROM {D_LOG}")  # the loss — on scratch

seller = f"{D_SILVER}.seller"
SELLER_COLUMNS = business_columns("seller")
night1 = typed_night("seller", bronze_night("seller", "2016-09-04", D_BRONZE)).select(*SELLER_COLUMNS)
now = spark.table(seller).select(*SELLER_COLUMNS)
back_in = night1.join(now.select("seller_id"), "seller_id", "left_anti").count()
put_back = night1.exceptAll(now).join(now.select("seller_id"), "seller_id").count()
expected_seller = [("2016-09-04", (back_in, put_back, 0))] + [
    (n, v) for (d, n), v in sorted(first_applied.items()) if d == "seller" and n != "2016-09-04"
]
print(f"predicted seller replay: {expected_seller}")

seller_before = spark.sql(f"DESCRIBE HISTORY {seller} LIMIT 1").first().version
customer_before = spark.sql(f"DESCRIBE HISTORY {D_SILVER}.customer LIMIT 1").first().version
try:
    run("customer", guard_reset=False)
    customer_refused = None
except MassDeleteRefused as e:
    customer_refused = str(e)
run("seller", guard_reset=False)

feed = change_counts(seller, seller_before + 1)
churn = {t: sum(v.get(t, 0) for v in feed.values()) for t in ("insert", "update_postimage", "delete")}
print(f"customer: {(customer_refused or 'NOT refused')[:250]}")
print(f"seller replay: {log_lines('seller')}\nseller change feed since the loss: {churn}")
print(f"seller vs production: {vs_production('seller')}; customer version {customer_before} -> "
      f"{spark.sql(f'DESCRIBE HISTORY {D_SILVER}.customer LIMIT 1').first().version}")

assert back_in == 100
assert customer_refused and log_lines("customer") == []
assert spark.sql(f"DESCRIBE HISTORY {D_SILVER}.customer LIMIT 1").first().version == customer_before
assert log_lines("seller") == expected_seller
assert churn["insert"] == 100 and churn["delete"] == 100
assert vs_production("seller") == (0, 0)
print("BUG PRESENT: customer stopped by the breaker; seller replayed its whole history over today's table "
      "with no error — 100 deleted sellers re-inserted and re-deleted in the change feed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## L2 — the guard: no line in the log, rows in Silver → refused before any MERGE

# COMMAND ----------

def silver_versions():
    return {dim: spark.sql(f"DESCRIBE HISTORY {D_SILVER}.{dim} LIMIT 1").first().version for dim in DIMS}


spark.sql(f"DELETE FROM {D_LOG}")  # lost again (L1 wrote five seller lines)
versions = silver_versions()
for dim in ("customer", "seller"):
    try:
        run(dim)
        refused = None
    except MergeLogLost as e:
        refused = str(e)
    print(f"{dim}: {(refused or 'NOT refused')[:200]}")
    assert refused
assert versions == silver_versions()

for dim in DIMS:  # production has its log: the daily run passes
    refuse_lost_log(dim, f"{SILVER}.{dim}", MERGE_LOG)
print("GUARD SEEN: refused before any MERGE; production's three dimensions pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ## L3 — the production repair: put the log back with time travel, then run as usual

# COMMAND ----------

display(spark.sql(f"RESTORE TABLE {D_LOG} TO VERSION AS OF {LOG_COMPLETE}"))
for dim in DIMS:
    run(dim)
after = silver_versions()
print(f"versions {versions} -> {after}; log lines {spark.table(D_LOG).count()}")
assert after == versions and spark.table(D_LOG).count() == 15
assert all(vs_production(dim) == (0, 0) for dim in DIMS)
print("REPAIRED: the restored log says where Silver is; nothing new, nothing written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## O — an old night re-delivered, corrected
# MAGIC
# MAGIC Night 2017-01-02 of seller arrives again with one city corrected (a newer file time, as Auto Loader
# MAGIC records a re-delivery). Silver shows night 2017-12-28 and never goes back in time, so `plan_nights`
# MAGIC only reports it. **The question is who hears about it.** Drill 2's first run: nobody — one printed
# MAGIC line in a green Job, and the lag alarm (which counted only nights *newer* than the last applied) at
# MAGIC 0. Since then the alarm also counts `old_nights_changed`; its Session 9 part (`dims_behind`) is
# MAGIC asserted still blind, so the bug stays visible next to the fix.

# COMMAND ----------

OLD_NIGHT = "2017-01-02"
old_night = spark.table(f"{D_BRONZE}.seller").where(F.col("dump_date") == OLD_NIGHT)
victim = old_night.orderBy("seller_id").first().seller_id
corrected = (
    old_night
    .withColumn("seller_city", F.when(F.col("seller_id") == victim, F.lit("drill2 corrected"))
                .otherwise(F.col("seller_city")))
    .withColumn("_source_modified_at", F.col("_source_modified_at") + F.expr("INTERVAL 1 DAY"))
)
(
    corrected.write.format("delta").mode("overwrite")
    .option("replaceWhere", f"dump_date = '{OLD_NIGHT}'").saveAsTable(f"{D_BRONZE}.seller")
)

plan = plan_nights("seller", D_BRONZE, D_LOG)
before = spark.sql(f"DESCRIBE HISTORY {seller} LIMIT 1").first().version
run("seller")  # raises nothing
alarm = lag_alarm()
print(f"plan: apply {plan[0]}, report {plan[1]}; seller version {before} -> "
      f"{spark.sql(f'DESCRIBE HISTORY {seller} LIMIT 1').first().version}; lag alarm {alarm.asDict()}")
assert plan == ([], [OLD_NIGHT])
assert spark.sql(f"DESCRIBE HISTORY {seller} LIMIT 1").first().version == before
assert alarm.dims_behind == 0, "the Session 9 rule should not see an old night"
assert (alarm.old_nights_changed, alarm.sources_behind) == (1, 1)
print("GUARD SEEN: Silver did not go back in time, and the alarm now says so (old_nights_changed 1); "
      "the Session 9 rule alone would have said 0")

# The response the alarm asks for: rebuild seller from an empty table. Every night is applied again,
# the corrected one included, and the merge log records it — which clears the count.
spark.sql(f"DELETE FROM {D_LOG} WHERE dimension = 'seller'")
spark.sql(f"TRUNCATE TABLE {seller}")
run("seller")
after = lag_alarm()
production_lines = sorted((n, v) for (d, n), v in first_applied.items() if d == "seller")
print(f"rebuilt seller: {log_lines('seller')}\nproduction:     {production_lines}\n"
      f"seller vs production, every column: {vs_production('seller')}; lag alarm {after.asDict()}")
assert after.sources_behind == 0
# Silver shows the NEWEST night, which the correction did not touch: the current state is unchanged. What
# changed is the record of history — that night's line in the merge log, and the change feed Gold reads.
assert vs_production("seller") == (0, 0)
assert log_lines("seller") != production_lines
print("CLEARED: the rebuild applied every night, the corrected one included, and the alarm is back to 0. "
      "Silver's current state did not move; only the history of 2017-01-02 did")

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {seller}").select("version", "timestamp", "operation", "operationMetrics")
)