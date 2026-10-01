# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
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

progress = run_stream(**STREAM_ARGS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — against the topic's own end offsets, then against what the topic is known to hold
# MAGIC
# MAGIC Messages are checked against the broker's end offsets for this run (`topic_end`), not against
# MAGIC numbers written into this notebook (see stream_orders). The laptop counted 158,625 on
# MAGIC 2026-09-27 (52,838 / 53,699 / 52,088): 158,346 clean events, 120 exact duplicates of clean
# MAGIC events, and 159 contaminated records (53 distinct `event_id`s, three test runs each). So Bronze
# MAGIC must hold 158,346 + 53 = **158,399** distinct `event_id`s.
# MAGIC
# MAGIC **Session 9** sends 100 delist events (`op = 'D'`), the first CDC messages with a provenance header
# MAGIC (scope `full;only=D`). Every labelled message must be a delist and every delist labelled; each is a
# MAGIC new event, so the distinct count becomes 158,399 + 100.

# COMMAND ----------

END = topic_end(progress)
print(f"topic end, per partition (broker): {END}  total {sum(END.values()):,}")
check_coordinates(TABLE, QUARANTINE, END)

facts = spark.sql(
    f"""
    SELECT count(*) AS rows,
           count(DISTINCT event_id) AS distinct_event_ids,
           count(DISTINCT sku_key) AS skus,
           count_if(op = 'I') AS inserts, count_if(op = 'U') AS updates, count_if(op = 'D') AS deletes,
           count(DISTINCT _schema_id) AS schema_ids,
           count_if({header_sql('ledgerline.run_id')} IS NOT NULL) AS labelled_rows,
           count_if({header_sql('ledgerline.scope')} = 'full;only=D') AS delist_scope_rows
    FROM {TABLE}
    """
).collect()[0]
print(facts)
assert facts.rows == sum(END.values())
assert facts.deletes in (0, 100), f"{facts.deletes} delete messages; the Session 9 delist run sends 100, once"
assert facts.labelled_rows == facts.delist_scope_rows == facts.deletes
assert facts.distinct_event_ids == 158_399 + facts.deletes

check_batches_not_doubled(TABLE)
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
assert recon.units_sold_clean == 112_650  # a delist moves no stock, so it sells nothing
assert recon.clean_events == 158_346 + facts.deletes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plain re-run — nothing new, nothing written

# COMMAND ----------

check_coordinates(TABLE, QUARANTINE, topic_end(run_stream(**STREAM_ARGS)))

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {TABLE}").select(
        "version", "timestamp", "operation", "operationParameters", "operationMetrics"
    )
)