# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bridge — export Silver's changes to S3 for Snowflake
# MAGIC
# MAGIC **Pattern:** each Silver table's Change Data Feed, from the version after its last export to now,
# MAGIC written once as Parquet to `s3://ledgerline-bridge-8176114c/silver/<table>/v<from>-<to>/`, then
# MAGIC logged in `workspace.bridge.export_log`. Snowflake loads the folders with `COPY INTO` / Snowpipe.
# MAGIC The code is in `_export_silver`.
# MAGIC
# MAGIC **How to run:** "Run all" with the widget `mode` on `plan` (the default) prints what each table
# MAGIC would export and writes nothing. Set `mode` to `export` and run again to write.
# MAGIC
# MAGIC **First export:** every table's whole change feed from version 0 — the complete history, so Gold
# MAGIC can rebuild the dims night by night (exp_05). Possible only while the oldest change-feed files
# MAGIC exist: Silver keeps them 30 days, and the dims began on 2026-09-30, so **the first export must run
# MAGIC before ~2026-10-30**.
# MAGIC
# MAGIC **Safe to re-run:** a range is never exported twice; a run with nothing new writes nothing.

# COMMAND ----------

# MAGIC %run ./_export_silver

# COMMAND ----------

dbutils.widgets.dropdown("mode", "plan", ["plan", "export"])
MODE = dbutils.widgets.get("mode")
create_log()

plans = [plan(name) for name in TABLES]
for p in plans:
    print(f"{p['table']:<17} versions {p['from']}..{p['to']}"
          f"{'  (nothing new)' if p['from'] > p['to'] else ''}"
          f"{'  ORPHAN FOLDERS ' + str(p['orphans']) if p['orphans'] else ''}")
print(f"\nmode = {MODE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export (only with `mode` = `export`)

# COMMAND ----------

if MODE == "export":
    for name in TABLES:
        line = export(name)
        print(f"{name:<17} {line if line else 'nothing new'}")
else:
    print("plan only — nothing written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify 1 — the log and the bucket agree: every logged folder exists, nothing unlogged

# COMMAND ----------

log = spark.table(EXPORT_LOG)
display(log.orderBy("table_name", "from_version"))
for name in TABLES:
    logged = {r.path for r in log.where(F.col("table_name") == name).where("path IS NOT NULL").collect()}
    found = set(existing_folders(name))
    print(f"{name:<17} logged folders {len(logged)}, in bucket {len(found)}")
    assert logged == found, f"{name}: log and bucket disagree: {logged ^ found}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify 2 — the files alone rebuild Silver, row for row
# MAGIC
# MAGIC Each key's newest change from the Parquet, deletes dropped, compared with Silver as of the last
# MAGIC exported version in both directions. This is the job Gold's MERGE does in Snowflake; if it fails
# MAGIC here, no Snowflake code can make it right.

# COMMAND ----------

for name in TABLES:
    if log.where(F.col("table_name") == name).where("path IS NOT NULL").count() == 0:
        print(f"{name:<17} nothing exported yet")
        continue
    result = compare_with_silver(name)
    print(f"{name:<17} {result}")
    assert result["only_in_rebuilt"] == result["only_in_silver"] == 0, f"{name}: files do not rebuild Silver"
    assert result["silver_rows"] == result["rebuilt_rows"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify 3 — a second export right now has nothing to do (only with `mode` = `export`)

# COMMAND ----------

if MODE == "export":
    # A commit that changed no row (Predictive Optimization's OPTIMIZE, seconds after a write) moves
    # the version without anything to export — only rows count.
    pending = {
        p["table"]: changes(p["table"], p["from"], p["to"]).count()
        for p in (plan(name) for name in TABLES) if p["from"] <= p["to"]
    }
    print(f"new versions since the export, rows in them: {pending}")
    assert sum(pending.values()) == 0, "Silver changed rows during the export, or a range was not logged"

# COMMAND ----------

# MAGIC %md
# MAGIC ## The run's answer, for Airflow (Session 16)
# MAGIC Every exported folder in the log and its row count — ALL of them, not only this run's — so the next
# MAGIC task ("wait until RAW holds it all") also waits for an earlier run's files when this run found
# MAGIC nothing new (a retry, a backfill). Read by `scripts/pipeline_step.py job ledgerline-bridge`.

# COMMAND ----------

import json  # noqa: E402 — a notebook cell, not a module header

summary = {
    f"{r.table_name}/{r.path.rstrip('/').rsplit('/', 1)[1]}": r.rows
    for r in spark.table(EXPORT_LOG).where("path IS NOT NULL").collect()
}
print(f"{len(summary)} exported folders, {sum(summary.values()):,} rows")
dbutils.notebook.exit(json.dumps(summary, sort_keys=True))