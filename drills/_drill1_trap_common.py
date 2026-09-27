# Databricks notebook source
# MAGIC %md
# MAGIC # Drill 1 — checkpoint-reset trap: shared names
# MAGIC
# MAGIC Loaded with `%run` by `drill1_trap_1_life1` and `drill1_trap_2_reset`. Defines names only.
# MAGIC **Scratch tables only** (`workspace.bronze.drill1_trap*`); the real Bronze tables are never
# MAGIC touched. Reads the live `orders` topic, read-only.

# COMMAND ----------

# MAGIC %run ../databricks/bronze/_kafka_bronze

# COMMAND ----------

TOPIC = "orders"
SUBJECT = "orders-value"
TRAP_TABLE = f"{CATALOG}.{SCHEMA}.drill1_trap"
TRAP_QUARANTINE = f"{TRAP_TABLE}_quarantine"
TRAP_STATE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/drill1/trap"

# A constant, not RUN_ID-based: life 1 and life 2 run in different notebook executions, and the
# whole trap is that the app id SURVIVES while the checkpoint does not.
APP_ID_V1 = "drill1.trap.v1"
CHECKPOINT_V1 = f"{TRAP_STATE}/v1"
# The safe reset: a new generation means a new checkpoint AND a new app id, into an empty table.
APP_ID_V2 = "drill1.trap.v2"
CHECKPOINT_V2 = f"{TRAP_STATE}/v2"


def trap_args(stream, checkpoint, app_id):
    return {
        "stream": stream,
        "topic": TOPIC,
        "subject": SUBJECT,
        "table": TRAP_TABLE,
        "quarantine_table": TRAP_QUARANTINE,
        "checkpoint": checkpoint,
        "app_id": app_id,
    }


def rm_if_present(path):
    try:
        dbutils.fs.rm(path, True)
    except Exception as e:  # nothing there yet is fine; anything else is real
        if not _missing_path(e):
            raise


def drop_scratch():
    for name in (TRAP_TABLE, TRAP_QUARANTINE):
        spark.sql(f"DROP TABLE IF EXISTS {name}")
    rm_if_present(TRAP_STATE)


def batch_files(checkpoint, kind):
    """Batch numbers that have a file in <checkpoint>/<kind>/ ("offsets" or "commits")."""
    try:
        return sorted(int(f.name) for f in dbutils.fs.ls(f"{checkpoint}/{kind}") if f.name.isdigit())
    except Exception as e:
        if _missing_path(e):
            return []
        raise


def table_end(table):
    """{partition: offset one past the highest one in the table} — what the table has seen."""
    return {
        r.p: r.end
        for r in spark.sql(
            f"SELECT _kafka_partition AS p, max(_kafka_offset) + 1 AS end FROM {table} GROUP BY 1"
        ).collect()
    }


def data_commits(table):
    """Commits that wrote rows. Predictive Optimization may add OPTIMIZE commits on its own."""
    history = spark.sql(f"DESCRIBE HISTORY {table}")
    return history.where("operation IN ('WRITE', 'STREAMING UPDATE')").count()
