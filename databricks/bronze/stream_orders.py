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

progress = run_stream(**STREAM_ARGS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — every message on the topic, once; every event of the dataset, at least once
# MAGIC
# MAGIC **Messages** are checked against the broker's own end offsets for this run (`topic_end`).
# MAGIC Session 7 checked against numbers the laptop counted (394,090; 131,458 / 132,187 / 130,445),
# MAGIC written into this notebook — correct that day, and wrong the moment a legitimate message
# MAGIC arrived. The laptop verifier is still the independent cross-check, run by hand.
# MAGIC
# MAGIC **Events** are a property of the dataset, not of the topic: the full order run is 394,090
# MAGIC distinct `event_id`s, 99,441 `created` events, 112,650 units. Since Drill 1 the topic also
# MAGIC holds deliberate duplicates — two 10-order partial runs, 37 messages each, every one labelled
# MAGIC with a `ledgerline.run_id` header. Bronze keeps them (it is the faithful record); Silver
# MAGIC removes them by `event_id`. So: every extra row must be a labelled one.

# COMMAND ----------

END = topic_end(progress)
print(f"topic end, per partition (broker): {END}  total {sum(END.values()):,}")
check_coordinates(TABLE, QUARANTINE, END)

facts = spark.sql(
    f"""
    WITH one_per_event AS (
      SELECT * FROM {TABLE}
      QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY _kafka_partition, _kafka_offset) = 1
    )
    SELECT (SELECT count(*) FROM {TABLE}) AS rows,
           (SELECT count(DISTINCT event_id) FROM {TABLE}) AS distinct_event_ids,
           (SELECT count_if({header_sql('ledgerline.run_id')} IS NOT NULL) FROM {TABLE}) AS labelled_rows,
           (SELECT count_if(event_type = 'created') FROM one_per_event) AS created,
           (SELECT sum(CASE WHEN event_type = 'created' THEN size(items) END) FROM one_per_event) AS units,
           (SELECT count(DISTINCT _schema_id) FROM {TABLE}) AS schema_ids
    """
).collect()[0]
print(facts)
assert facts.rows == sum(END.values())
assert facts.distinct_event_ids == 394_090
# Every duplicate came from a labelled run, and every labelled row is a duplicate.
assert facts.rows - facts.distinct_event_ids == facts.labelled_rows
assert facts.created == 99_441
assert facts.units == 112_650

check_batches_not_doubled(TABLE)
check_backlog(STREAM)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Who sent the extra messages — one query, thanks to the provenance header

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT {header_sql('ledgerline.run_id')} AS run_id,
               {header_sql('ledgerline.producer')} AS producer,
               {header_sql('ledgerline.scope')} AS scope,
               count(*) AS messages, count(DISTINCT event_id) AS events,
               min(_kafka_timestamp) AS first_at, max(_kafka_timestamp) AS last_at
        FROM {TABLE}
        GROUP BY ALL ORDER BY first_at
        """
    )
)

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

check_coordinates(TABLE, QUARANTINE, topic_end(run_stream(**STREAM_ARGS)))

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
