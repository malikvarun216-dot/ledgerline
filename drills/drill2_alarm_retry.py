# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Drill 2 — after the alarm: what a refused batch leaves, and the person's fix
# MAGIC
# MAGIC Run **after** the Job that ran `drill2_alarm_task` has failed (and its email has arrived). Safe to
# MAGIC "Run all": every stream here is expected to succeed.
# MAGIC
# MAGIC 1. **What the failure left** — nothing in the three Silver tables; in the checkpoint, the plan for
# MAGIC    batch 0 (`offsets/0`) but no record that it finished (`commits/0`). So the next run retries
# MAGIC    exactly that batch — nothing skipped, nothing half-written.
# MAGIC 2. **The fix, as production would do it** — a person decides the moved copy is the wrong event and
# MAGIC    denylists it (in production: a file under `ops/incidents/`, in a commit). Same checkpoint, same
# MAGIC    app id: the stream retries batch 0 and applies the real event.

# COMMAND ----------

# MAGIC %run ../databricks/silver/_merge_orders

# COMMAND ----------

A = {k: f"{SILVER}.drill2_alarm_{k}" for k in ("bronze", "orders", "items", "log")}
CHECKPOINT = f"{CHECKPOINTS}/drill2/alarm"
APP_ID = "ledgerline.drill2.alarm"


def listing(folder):
    try:
        return sorted(f.name.rstrip("/") for f in dbutils.fs.ls(f"{CHECKPOINT}/{folder}"))
    except Exception as e:
        if _missing_path(e):
            return []
        raise


left = {t: spark.table(A[t]).count() for t in ("orders", "items", "log")}
print(f"rows left by the failed run: {left}; "
      f"checkpoint offsets {listing('offsets')}, commits {listing('commits')}")
assert left == {"orders": 0, "items": 0, "log": 0}, "the refused batch wrote something"
assert listing("offsets") == ["0"] and listing("commits") == []

# COMMAND ----------

bronze = spark.table(A["bronze"])
in_production = spark.table(BRONZE_ORDERS).select("event_id")
wrong = [r.event_id for r in bronze.join(in_production, "event_id", "left_anti").collect()]
assert len(wrong) == 1
real = bronze.join(in_production, "event_id").first()

run_silver_orders(
    checkpoint=CHECKPOINT, app_id=APP_ID, source=A["bronze"], denylist=wrong,
    orders_table=A["orders"], items_table=A["items"], log_table=A["log"],
)
line = spark.table(A["log"]).first()
order = spark.table(A["orders"]).first()
units = len(real["items"] or [])
print(f"denylisted {wrong}\nlog: {line.asDict()}\norder: {order.order_id} created_at {order.created_at} "
      f"(the real event says {real.event_ts}); items {spark.table(A['items']).count()} of {units}")
print(f"checkpoint commits {listing('commits')}")

assert (line.batch_id, line.bronze_rows, line.events, line.inserted, line.duplicate_copies) == (0, 2, 1, 1, 0)
assert spark.table(A["orders"]).count() == 1 and order.created_at == real.event_ts
assert spark.table(A["items"]).count() == units
assert listing("commits") == ["0"]
print("FIX SEEN: the same batch, retried from the same checkpoint with one event denylisted, applied the "
      "real event — and only it")