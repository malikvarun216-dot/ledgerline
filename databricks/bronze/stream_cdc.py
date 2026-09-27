# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — inventory CDC from Kafka topic `inventory.cdc`
# MAGIC
# MAGIC Append-only raw op log. **Bronze does not interpret the ops**: an `I`, `U` or `D` is stored as
# MAGIC a row like any other. Applying them to current stock is Silver's job (Session 9).
# MAGIC
# MAGIC **Ingested unfiltered, including the 159 records the test suite put on the topic by mistake**
# MAGIC (incidents.md, 2026-09-26). Bronze is the faithful record of what the topic holds, mistake
# MAGIC included. Silver excludes them using the published denylist
# MAGIC `ops/incidents/2026-09-26_inventory_cdc_denylist.json` — the topic itself is never edited.

# COMMAND ----------

# MAGIC %run ./_kafka_bronze

# COMMAND ----------

import json
import os

TOPIC = "inventory.cdc"
SUBJECT = "inventory.cdc-value"
STREAM = "bronze.inventory_cdc"
TABLE = f"{CATALOG}.{SCHEMA}.inventory_cdc"
QUARANTINE = f"{CATALOG}.{SCHEMA}.inventory_cdc_quarantine"

# Same rule as stream_orders: the checkpoint and the app id share a generation, and a checkpoint
# is never deleted in place.
GENERATION = "v1"
CHECKPOINT = f"{STATE}/inventory_cdc/{GENERATION}"
APP_ID = f"ledgerline.bronze.inventory_cdc.{GENERATION}"

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
# MAGIC ## Verify — against the topic, counted independently on a laptop
# MAGIC
# MAGIC The topic holds 158,625 records: 158,346 clean events, 120 exact duplicates of clean events,
# MAGIC and 159 contaminated records (53 distinct `event_id`s, three test runs each). So Bronze must hold
# MAGIC 158,625 rows and 158,346 + 53 = **158,399** distinct `event_id`s.

# COMMAND ----------

PER_PARTITION = {0: 52_838, 1: 53_699, 2: 52_088}

check_coordinates(TABLE, QUARANTINE, PER_PARTITION)

facts = spark.sql(
    f"""
    SELECT count(*) AS rows,
           count(DISTINCT event_id) AS distinct_event_ids,
           count(DISTINCT sku_key) AS skus,
           count_if(op = 'I') AS inserts, count_if(op = 'U') AS updates, count_if(op = 'D') AS deletes,
           count(DISTINCT _schema_id) AS schema_ids
    FROM {TABLE}
    """
).collect()[0]
print(facts)
assert facts.rows == 158_625
assert facts.distinct_event_ids == 158_399

check_batches_not_doubled(TABLE, APP_ID)
check_backlog(STREAM)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The contamination is in Bronze, exactly as measured — and the denylist accounts for it
# MAGIC
# MAGIC Session 6 measured, from a laptop: units sold computed from the raw log = **112,806**; after
# MAGIC dropping the denylisted events and keeping one copy per `event_id` = **112,650**, which is the
# MAGIC true figure (it equals the units on the `orders` topic). Recomputed here from Bronze, by a
# MAGIC different engine. Units sold = sum of every stock *decrease* (`prev_stock_qty - stock_qty`),
# MAGIC the same rule `scripts/inspect_stream.py` uses.
# MAGIC
# MAGIC This does not filter Bronze. It proves the denylist matches what Bronze actually holds, so
# MAGIC Silver can rely on it.

# COMMAND ----------

# A Git-folder notebook runs with its own folder as the working directory.
DENYLIST_PATH = os.path.abspath(
    os.path.join(os.getcwd(), "../../ops/incidents/2026-09-26_inventory_cdc_denylist.json")
)
assert os.path.exists(DENYLIST_PATH), f"denylist not found at {DENYLIST_PATH} (cwd={os.getcwd()})"
with open(DENYLIST_PATH) as fh:
    denylist = json.load(fh)
spark.createDataFrame([(e,) for e in denylist["event_ids"]], "event_id STRING").createOrReplaceTempView(
    "cdc_denylist"
)

recon = spark.sql(
    f"""
    WITH flagged AS (
      SELECT b.*, d.event_id IS NOT NULL AS denied
      FROM {TABLE} b LEFT JOIN cdc_denylist d ON b.event_id = d.event_id
    ),
    one_per_event AS (
      SELECT * FROM flagged WHERE NOT denied
      QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY _kafka_partition, _kafka_offset) = 1
    )
    SELECT
      (SELECT count_if(denied) FROM flagged) AS denied_rows,
      (SELECT count(DISTINCT event_id) FILTER (WHERE denied) FROM flagged) AS denied_event_ids,
      (SELECT sum(CASE WHEN prev_stock_qty > stock_qty THEN prev_stock_qty - stock_qty ELSE 0 END)
         FROM flagged) AS units_sold_raw,
      (SELECT sum(CASE WHEN prev_stock_qty > stock_qty THEN prev_stock_qty - stock_qty ELSE 0 END)
         FROM one_per_event) AS units_sold_clean,
      (SELECT count(*) FROM one_per_event) AS clean_events
    """
).collect()[0]
print(recon)
assert len(denylist["event_ids"]) == 53
assert recon.denied_rows == 159
assert recon.denied_event_ids == 53
assert recon.units_sold_raw == 112_806
assert recon.units_sold_clean == 112_650
assert recon.clean_events == 158_346

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plain re-run — nothing new, nothing written

# COMMAND ----------

run_stream(**STREAM_ARGS)
check_coordinates(TABLE, QUARANTINE, PER_PARTITION)

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {TABLE}").select(
        "version", "timestamp", "operation", "operationParameters", "operationMetrics"
    )
)
