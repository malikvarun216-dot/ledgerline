# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Re-apply the newest night of one dimension — after Silver's typing changed (Session 22)
# MAGIC
# MAGIC `merge_dims` applies a night once: the merge log says which night Silver shows, and a night already
# MAGIC applied is never applied again unless Bronze holds a newer copy of it. That is right for data — and
# MAGIC wrong when **Silver's own rule changed** (S22: review text is now redacted), because then the same
# MAGIC Bronze night should give different Silver rows, and nothing would ever apply it.
# MAGIC
# MAGIC This runs the **same** `apply_night` (contract, delete breaker, MERGE, merge-log line) on the newest
# MAGIC night only. The MERGE updates only rows whose typed values differ, so the change is exactly the rows
# MAGIC the new rule touches — counted in `plan` first. Only the newest night: an older one would take
# MAGIC Silver back in time.
# MAGIC
# MAGIC **Run with `mode` = `plan` first.**

# COMMAND ----------

# MAGIC %run ../silver/_merge_dims

# COMMAND ----------

dbutils.widgets.text("dimension", "review")
dbutils.widgets.dropdown("mode", "plan", ["plan", "apply"])
DIM = dbutils.widgets.get("dimension")
MODE = dbutils.widgets.get("mode")
TABLE = f"{SILVER}.{DIM}"

newest = spark.table(f"{BRONZE}.{DIM}").agg(F.max("dump_date")).first()[0]
logged = spark.table(MERGE_LOG).where(F.col("dimension") == DIM).agg(
    F.max(F.date_format("dump_date", "yyyy-MM-dd"))
).first()[0]
assert newest == logged, f"{DIM}: Silver shows {logged}, Bronze's newest is {newest} — run merge_dims first"

typed = typed_night(DIM, bronze_night(DIM, newest))
extra, missing = compare_to_night(TABLE, typed, DIM)
print(f"{DIM} night {newest}: rows Silver holds that the new typing would change: {extra:,} "
      f"(night rows that differ from Silver: {missing:,}); mode = {MODE}")

# COMMAND ----------

if MODE == "apply" and (extra or missing):
    before = latest_version(TABLE)
    apply_night(DIM, newest, TABLE)
    after_extra, after_missing = compare_to_night(TABLE, typed_night(DIM, bronze_night(DIM, newest)), DIM)
    print(f"after: extra={after_extra} missing={after_missing}; version {before} -> {latest_version(TABLE)}")
    assert (after_extra, after_missing) == (0, 0), "Silver still differs from the night"
elif MODE == "apply":
    print("nothing differs — nothing applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What changed, from the change feed (the rows the bridge will export)

# COMMAND ----------

# The newest MERGE, found by operation: Predictive Optimization may have committed after it.
last = spark.sql(f"DESCRIBE HISTORY {TABLE}").where("operation = 'MERGE'").agg(F.max("version")).first()[0]
display(
    spark.sql(f"SELECT _commit_version, _change_type, count(*) AS n FROM table_changes('{TABLE}', {last}) "
              "GROUP BY 1, 2 ORDER BY 1, 2")
)