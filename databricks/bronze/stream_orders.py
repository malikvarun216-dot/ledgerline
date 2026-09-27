# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — order events from Kafka topic `orders`
# MAGIC
# MAGIC Append-only, exactly-once. Every message on the topic becomes one row, once, with its Kafka
# MAGIC coordinates. Order events are immutable facts, so Bronze never updates or deduplicates them —
# MAGIC a producer that sent the same event twice gets two rows here, and Silver's `MERGE` on
# MAGIC `event_id` is where that is resolved (Session 8).
# MAGIC
# MAGIC Shared machinery (read, decode, write, monitor, verify) is in `_kafka_bronze`.

# COMMAND ----------

# MAGIC %run ./_kafka_bronze

# COMMAND ----------

TOPIC = "orders"
SUBJECT = "orders-value"
STREAM = "bronze.orders"
TABLE = f"{CATALOG}.{SCHEMA}.orders"
QUARANTINE = f"{CATALOG}.{SCHEMA}.orders_quarantine"

# The checkpoint and the Delta app id carry the same generation, and a checkpoint is never
# deleted in place. Delete it and keep the app id, and the new stream counts batches from 0
# again while Delta remembers this app id at version ~7 — so it silently skips batches 0-7
# as "already written". Drill 1 demonstrates exactly that on purpose.
# A new generation is a new stream from "earliest": it re-reads the whole topic, so it is a
# rebuild of the table, never a quiet restart.
GENERATION = "v1"
CHECKPOINT = f"{STATE}/{TOPIC}/{GENERATION}"
APP_ID = f"ledgerline.bronze.{TOPIC}.{GENERATION}"

STREAM_ARGS = {
    "stream": STREAM,
    "topic": TOPIC,
    "subject": SUBJECT,
    "table": TABLE,
    "quarantine_table": QUARANTINE,
    "checkpoint": CHECKPOINT,
    "app_id": APP_ID,
}

run_stream(**STREAM_ARGS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — against the topic itself, counted independently on a laptop
# MAGIC
# MAGIC `scripts/inspect_topic.py` read the live topic without Spark: 394,090 records, 394,090 distinct
# MAGIC `event_id`, 131,458 / 132,187 / 130,445 per partition, 99,441 `created` events carrying
# MAGIC 112,650 units. Bronze must reproduce every one of those numbers.

# COMMAND ----------

PER_PARTITION = {0: 131_458, 1: 132_187, 2: 130_445}

check_coordinates(TABLE, QUARANTINE, PER_PARTITION)

facts = spark.sql(
    f"""
    SELECT count(*) AS rows,
           count(DISTINCT event_id) AS distinct_event_ids,
           count_if(event_type = 'created') AS created,
           sum(CASE WHEN event_type = 'created' THEN size(items) END) AS units,
           count(DISTINCT _schema_id) AS schema_ids,
           min(event_ts) AS first_event, max(event_ts) AS last_event,
           min(produced_at) AS first_produced, max(produced_at) AS last_produced
    FROM {TABLE}
    """
).collect()[0]
print(facts)
assert facts.rows == 394_090
assert facts.distinct_event_ids == 394_090
assert facts.created == 99_441
assert facts.units == 112_650

check_batches_not_doubled(TABLE, APP_ID)
check_backlog(STREAM)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine path — driven on purpose, since the topic has no bad records

# COMMAND ----------

quarantine_self_test(TABLE, SUBJECT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plain re-run — nothing new on the topic, so nothing may be written
# MAGIC
# MAGIC The checkpoint says every offset is done. Expect 0 batches, and every count unchanged.

# COMMAND ----------

run_stream(**STREAM_ARGS)
check_coordinates(TABLE, QUARANTINE, PER_PARTITION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What happened — each batch is its own commit

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {TABLE}").select(
        "version", "timestamp", "operation", "operationParameters", "operationMetrics"
    )
)

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {PROGRESS_TABLE} WHERE stream = '{STREAM}' ORDER BY recorded_at, batch_id"))
