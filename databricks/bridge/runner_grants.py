# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bridge — what `ledgerline-jobs` needs to run the export as a job (Session 16)
# MAGIC
# MAGIC Until Session 16 a person ran `export_silver` by hand, as the workspace owner, who passes every
# MAGIC grant. Airflow now starts it as the bundle job `ledgerline-bridge`, which **runs as**
# MAGIC `ledgerline-jobs` (decisions.md, Session 12: one robot deploys, another runs). That robot has only
# MAGIC what is granted here. Run once, by the owner, before the job's first run; safe to re-run.
# MAGIC
# MAGIC Session 12's rule: a deploy is done only when one run **as the run-as identity** is green; and
# MAGIC Session 15's incident: a missing grant is found by the first run that needs it, so every grant
# MAGIC the export uses is listed here, with what uses it.

# COMMAND ----------

RUNNER = "18160145-ee5d-427a-8553-ef099e1ec870"  # ledgerline-jobs — an id, not a secret
for statement in (
    f"GRANT USE CATALOG ON CATALOG workspace TO `{RUNNER}`",
    # read every Silver table's change feed (and Silver itself, for Verify 2's rebuild check)
    f"GRANT USE SCHEMA, SELECT ON SCHEMA workspace.silver TO `{RUNNER}`",
    # read and append the export log
    f"GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA workspace.bridge TO `{RUNNER}`",
    # write the Parquet folders, and list them (Verify 1, the orphan check)
    f"GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ledgerline_bridge TO `{RUNNER}`",
):
    spark.sql(statement)
    print(statement)
SECURABLES = ("SCHEMA workspace.silver", "SCHEMA workspace.bridge", "EXTERNAL LOCATION ledgerline_bridge")
for securable in SECURABLES:
    rows = spark.sql(f"SHOW GRANTS ON {securable}").where(f"Principal = '{RUNNER}'").collect()
    print(f"{securable}: {sorted(r.ActionType for r in rows)}")