# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Drill 2 — alarm regression: Silver orders refuses a batch inside a real stream
# MAGIC
# MAGIC **Runs as a Job task, and is meant to FAIL.** Never "Run all" it in a notebook: a stream that dies
# MAGIC inside a notebook command fails the command (incidents.md 2026-09-27) — in a Job, that failure is the
# MAGIC test.
# MAGIC
# MAGIC Session 10 showed the orders refusal (`find_problems` → `BatchRefused`) read-only, and exp_04 showed
# MAGIC the CDC one by calling the batch function directly. Neither has fired inside a running stream, and
# MAGIC none has ever reached the failure email. Here, end to end, on scratch tables
# MAGIC `workspace.silver.drill2_alarm_*`:
# MAGIC
# MAGIC 1. Scratch Bronze gets one real `created` event and a copy of it one hour later under a new
# MAGIC    `event_id` — two different times for one step of one order.
# MAGIC 2. The production stream (`run_silver_orders`) reads it: batch 0 is refused, nothing written, the
# MAGIC    stream stops, **the task fails, the Job emails**.
# MAGIC 3. Afterwards, `drill2_alarm_retry` checks what the failure left and applies the person's fix (a
# MAGIC    denylist entry), retrying the same batch from the same checkpoint.
# MAGIC
# MAGIC Every attempt rebuilds the scratch state first, so the Job's automatic second attempt fails the
# MAGIC same way.

# COMMAND ----------

# MAGIC %run ../databricks/silver/_merge_orders

# COMMAND ----------

A = {k: f"{SILVER}.drill2_alarm_{k}" for k in ("bronze", "orders", "items", "log")}
CHECKPOINT = f"{CHECKPOINTS}/drill2/alarm"
APP_ID = "ledgerline.drill2.alarm"

for table in A.values():
    spark.sql(f"DROP TABLE IF EXISTS {table}")
try:
    dbutils.fs.rm(CHECKPOINT, True)  # scratch only: each attempt starts from the same state
except Exception as e:
    if not _missing_path(e):
        raise

real = spark.table(BRONZE_ORDERS).where("event_type = 'created'").orderBy("event_id").limit(1)
moved = real.withColumn(
    "event_id", F.sha2(F.concat(F.col("event_id"), F.lit("-moved")), 256).substr(1, 32)
).withColumn("event_ts", F.col("event_ts") + F.expr("INTERVAL 1 HOUR"))
real.unionByName(moved).write.saveAsTable(A["bronze"])
landed = spark.table(A["bronze"]).collect()
print(f"scratch Bronze: {[(r.order_id, r.event_id, str(r.event_ts)) for r in landed]}")

# COMMAND ----------

# Expected to raise: batch 0: {'two_events_one_step': 1}
run_silver_orders(
    checkpoint=CHECKPOINT, app_id=APP_ID, source=A["bronze"],
    orders_table=A["orders"], items_table=A["items"], log_table=A["log"],
)
raise AssertionError("the stream applied a batch with two times for one step — the refusal did not fire")