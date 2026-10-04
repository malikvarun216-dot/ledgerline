# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Check — the Lakeflow pipeline against production Silver, row by row
# MAGIC
# MAGIC The pipeline `ledgerline-dlt` (`databricks/dlt/`) declares with AUTO CDC what production Silver
# MAGIC inventory does by hand. Both read the same Bronze rows, so they must agree — or the difference is
# MAGIC the finding. **Writes nothing.** Run it after a pipeline update.
# MAGIC
# MAGIC The first cell computes every expected number from Bronze and production Silver **only**, before
# MAGIC any pipeline table is read. The cells after it compare.

# COMMAND ----------

# MAGIC %run ../silver/_merge_inventory_cdc

# COMMAND ----------

DLT = "workspace.silver_dlt"
CHECKED = f"{DLT}.inventory_cdc_checked"
SCD1 = f"{DLT}.inventory_scd1"
SCD2 = f"{DLT}.inventory_scd2"
ORDERS_CHECKED = f"{DLT}.orders_checked"
BRONZE_ORDERS = "workspace.bronze.orders"

DENYLIST = load_denylist()
bronze = spark.table(BRONZE_CDC).agg(
    F.count(F.lit(1)).alias("rows"),
    F.count_if(F.col("event_id").isin(DENYLIST)).alias("denied"),
).first()
ev = spark.table(EVENTS).agg(
    F.count_if(F.col("op") != "D").alias("iu"), F.count_if(F.col("op") == "D").alias("d")
).first()
stock = spark.table(STOCK).agg(F.count(F.lit(1)).alias("skus"), F.sum("stock_qty").alias("units")).first()
orders = spark.table(BRONZE_ORDERS).agg(
    F.count(F.lit(1)).alias("rows"),
    F.count_if(F.expr("event_type = 'created' AND items IS NULL")).alias("created_no_items"),
    F.count_if(F.expr("order_status IS NULL OR trim(order_status) = ''")).alias("blank_status"),
).first()

PREDICTED = {
    "checked rows": bronze.rows - bronze.denied,  # every Bronze row but the denylisted copies
    "dropped by not_denylisted": bronze.denied,
    "scd1 skus": stock.skus,                       # = production silver.inventory
    "scd1 units": stock.units,
    "scd2 versions": ev.iu,                        # one version per I/U event Silver kept once
    "scd2 open": stock.skus,                       # the current versions = the current stock
    "scd2 closed by a delete": ev.d,
    "orders rows (warn)": orders.rows,             # `warn` keeps every row
    "created_has_items failures": orders.created_no_items,
    "order_status_present failures": orders.blank_status,
}
for name, value in PREDICTED.items():
    print(f"{name:>30}: {value:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — `inventory_cdc_checked`: what the expectations let through

# COMMAND ----------

checked = spark.table(CHECKED).agg(
    F.count(F.lit(1)).alias("rows"),
    F.count_distinct("event_id").alias("events"),
    F.count_if(F.col("event_id").isin(DENYLIST)).alias("denied_left"),
).first()
print(f"checked: {checked.asDict()}")
assert checked.rows == PREDICTED["checked rows"]
assert checked.denied_left == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — SCD1 = production `silver.inventory`, SKU by SKU
# MAGIC
# MAGIC A `FULL OUTER JOIN`: a SKU on one side only is a disagreement too (a missed delete, a resurrected
# MAGIC SKU), and an inner join would hide it.

# COMMAND ----------

scd1_vs_silver = spark.sql(f"""
    SELECT count(*)                                                     AS skus,
           count_if(p.sku_key IS NULL)                                  AS only_in_dlt,
           count_if(d.sku_key IS NULL)                                  AS only_in_silver,
           count_if(p.stock_qty <> d.stock_qty OR p.seq <> d.seq)       AS different,
           sum(d.stock_qty)                                             AS dlt_units
    FROM {STOCK} p FULL OUTER JOIN {SCD1} d ON p.sku_key = d.sku_key
""").first()
print(f"SCD1 vs silver.inventory: {scd1_vs_silver.asDict()}")
assert (scd1_vs_silver.only_in_dlt, scd1_vs_silver.only_in_silver, scd1_vs_silver.different) == (0, 0, 0)
assert scd1_vs_silver.skus == PREDICTED["scd1 skus"]
assert scd1_vs_silver.dlt_units == PREDICTED["scd1 units"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — SCD2 = the event log's own timeline
# MAGIC
# MAGIC Each version of a SKU starts at one event's `seq` and ends at the **next** event's `seq` — a
# MAGIC `lead()` over production's event log, ordered by `seq`. A `D` ends the version before it and opens
# MAGIC none. So the expected SCD2 table can be written in one window function; AUTO CDC must produce
# MAGIC exactly that, version by version.

# COMMAND ----------

scd2_vs_log = spark.sql(f"""
    WITH timeline AS (
        SELECT sku_key, event_id, op, seq AS start_at,
               lead(seq) OVER (PARTITION BY sku_key ORDER BY seq) AS end_at,
               lead(op)  OVER (PARTITION BY sku_key ORDER BY seq) AS next_op
        FROM {EVENTS}
    ),
    expected AS (SELECT * FROM timeline WHERE op <> 'D')
    SELECT count(*)                                                     AS versions,
           count_if(v.sku_key IS NULL)                                  AS missing_in_dlt,
           count_if(e.sku_key IS NULL)                                  AS extra_in_dlt,
           count_if(NOT (e.end_at <=> v.__END_AT))                      AS end_differs,
           count_if(e.event_id <> v.event_id)                           AS event_differs,
           count_if(v.__END_AT IS NULL)                                 AS open,
           count_if(e.next_op = 'D')                                    AS closed_by_delete
    FROM expected e FULL OUTER JOIN {SCD2} v
      ON e.sku_key = v.sku_key AND e.start_at = v.__START_AT
""").first()
print(f"SCD2 vs lead() over the event log: {scd2_vs_log.asDict()}")
same_start = spark.sql(f"""
    SELECT count(*) AS pairs FROM (
        SELECT sku_key, __START_AT FROM {SCD2} GROUP BY ALL HAVING count(*) > 1)
""").first().pairs
print(f"two versions with one start: {same_start}")
assert (scd2_vs_log.missing_in_dlt, scd2_vs_log.extra_in_dlt) == (0, 0)
assert (scd2_vs_log.end_differs, scd2_vs_log.event_differs, same_start) == (0, 0, 0)
assert scd2_vs_log.versions == PREDICTED["scd2 versions"]
assert scd2_vs_log.open == PREDICTED["scd2 open"]
assert scd2_vs_log.closed_by_delete == PREDICTED["scd2 closed by a delete"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — the open SCD2 versions = SCD1
# MAGIC
# MAGIC Two declarations over one source; "what is current" must not depend on which one you ask.

# COMMAND ----------

open_vs_scd1 = spark.sql(f"""
    SELECT count(*) AS skus,
           count_if(a.sku_key IS NULL OR b.sku_key IS NULL OR a.stock_qty <> b.stock_qty) AS different
    FROM (SELECT sku_key, stock_qty FROM {SCD2} WHERE __END_AT IS NULL) a
    FULL OUTER JOIN {SCD1} b ON a.sku_key = b.sku_key
""").first()
print(f"open SCD2 vs SCD1: {open_vs_scd1.asDict()}")
assert open_vs_scd1.different == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — the expectations, as the pipeline counted them
# MAGIC
# MAGIC Every pipeline writes an **event log**: one row per thing that happened in an update. A
# MAGIC `flow_progress` row carries, per expectation, how many rows passed and failed. Read with the
# MAGIC `event_log()` table function; one line per update and expectation.

# COMMAND ----------


def expectations(table):
    return spark.sql(f"""
        WITH progress AS (
            SELECT origin.update_id AS update_id, timestamp,
                   from_json(details:flow_progress.data_quality.expectations,
                             'array<struct<name: string, dataset: string,
                                           passed_records: bigint, failed_records: bigint>>') AS checks
            FROM event_log(TABLE({table}))
            WHERE event_type = 'flow_progress'
              AND details:flow_progress.data_quality.expectations IS NOT NULL
        )
        SELECT update_id, min(timestamp) AS first_seen, c.name,
               sum(c.passed_records) AS passed, sum(c.failed_records) AS failed
        FROM progress LATERAL VIEW explode(checks) AS c
        GROUP BY update_id, c.name
        ORDER BY first_seen, c.name
    """)


display(expectations(CHECKED))
display(expectations(ORDERS_CHECKED))

# COMMAND ----------

orders_checked = spark.table(ORDERS_CHECKED).agg(
    F.count(F.lit(1)).alias("rows"),
    F.count_if(F.expr("event_type = 'created' AND items IS NULL")).alias("created_no_items"),
).first()
print(f"orders_checked: {orders_checked.asDict()}")
assert orders_checked.rows == PREDICTED["orders rows (warn)"], "rows missing — was the rule `drop`?"
assert orders_checked.created_no_items == PREDICTED["created_has_items failures"]