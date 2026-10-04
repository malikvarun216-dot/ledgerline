# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Drill 2 — end-to-end reconciliation: every row and every unit, Bronze → Silver, both sources
# MAGIC
# MAGIC The project's headline check is that the two topics agree: units ordered = units the stock feed
# MAGIC decremented (112,650). Each layer already checks its own part. This notebook puts the whole chain in
# MAGIC one place, read-only, so every row and unit that leaves between raw Bronze and Silver is **named**:
# MAGIC which step removed it and why. Expected: each step's numbers add up exactly, both ways.
# MAGIC
# MAGIC | Source | Raw Bronze | removed by | Silver |
# MAGIC |---|---|---|---|
# MAGIC | inventory CDC | 158,725 rows, 112,806 units | denylist, duplicates | 158,446 events, 112,650 units |
# MAGIC | orders | 394,164 rows | duplicates (Drill 1's 74) | 99,441 orders, 112,650 items |

# COMMAND ----------

# MAGIC %run ../databricks/silver/_merge_inventory_cdc

# COMMAND ----------

DENYLIST = load_denylist()
assert len(DENYLIST) == 53
first_copy = Window.partitionBy("event_id").orderBy(*COORDINATES)
DROP = F.col("prev_stock_qty") - F.col("stock_qty")
SOLD = F.when(F.col("prev_stock_qty") > F.col("stock_qty"), DROP).otherwise(0)  # units sold, as Bronze counts

cdc = (
    spark.table(BRONZE_CDC)
    .withColumn("_denied", F.col("event_id").isin(DENYLIST))
    .withColumn("_copy", F.row_number().over(first_copy))
    .withColumn("_sold", SOLD)
)
dup = ~F.col("_denied") & (F.col("_copy") > 1)
c = cdc.agg(
    F.count(F.lit(1)).alias("rows"),
    F.sum("_sold").alias("units"),
    F.count_if("_denied").alias("denied_rows"),
    F.sum(F.when(F.col("_denied"), F.col("_sold")).otherwise(0)).alias("denied_units"),
    F.count_distinct(F.when(F.col("_denied"), F.col("event_id"))).alias("denied_ids"),
    F.count_if(dup).alias("dup_rows"),
    F.sum(F.when(dup, F.col("_sold")).otherwise(0)).alias("dup_units"),
).first()
silver_events = spark.table(EVENTS).agg(F.count(F.lit(1)).alias("rows"), F.sum(SOLD).alias("units")).first()

print("inventory CDC                 rows      units")
print(f"  raw Bronze             {c.rows:>9,} {c.units:>10,}")
print(f"  - denylisted           {-c.denied_rows:>9,} {-c.denied_units:>10,}   ({c.denied_ids} events)")
print(f"  - duplicate copies     {-c.dup_rows:>9,} {-c.dup_units:>10,}")
print(f"  = Silver event log     {silver_events.rows:>9,} {silver_events.units:>10,}")

assert (c.rows, c.units, c.denied_rows, c.denied_ids, c.dup_rows) == (158_725, 112_806, 159, 53, 120)
assert c.rows - c.denied_rows - c.dup_rows == silver_events.rows == 158_446
assert c.units - c.denied_units - c.dup_units == silver_events.units == 112_650

# COMMAND ----------

orders = (
    spark.table("workspace.bronze.orders")
    .withColumn("_copy", F.row_number().over(first_copy))
    .withColumn(
        "_units",
        # size(NULL) is -1 or NULL depending on ANSI mode; the production fold says 0 explicitly too.
        F.when((F.col("event_type") == "created") & F.col("items").isNotNull(), F.size("items")).otherwise(0),
    )
)
o = orders.agg(
    F.count(F.lit(1)).alias("rows"),
    F.sum("_units").alias("units"),
    F.count_if(F.col("_copy") > 1).alias("dup_rows"),
    F.sum(F.when(F.col("_copy") > 1, F.col("_units")).otherwise(0)).alias("dup_units"),
    F.count_if((F.col("_copy") == 1) & (F.col("event_type") == "created")).alias("created"),
).first()
silver_orders = spark.table("workspace.silver.orders").count()
silver_items = spark.table("workspace.silver.order_items").count()

print("orders                        rows      units")
print(f"  raw Bronze             {o.rows:>9,} {o.units:>10,}")
print(f"  - duplicate copies     {-o.dup_rows:>9,} {-o.dup_units:>10,}")
print(f"  = events               {o.rows - o.dup_rows:>9,} {o.units - o.dup_units:>10,}")
print(f"  Silver: {silver_orders:,} orders (= {o.created:,} created events), {silver_items:,} items")

assert (o.rows, o.dup_rows) == (394_164, 74)
assert o.created == silver_orders == 99_441
assert o.units - o.dup_units == silver_items == 112_650

# COMMAND ----------

# MAGIC %md
# MAGIC ## The two sources agree, per SKU — Silver order items vs Silver stock decrements
# MAGIC
# MAGIC Per SKU, not only the total: two SKUs off by opposite amounts cancel in a sum. The same check runs
# MAGIC daily as `merge_orders` Verify 7; a mismatch fails the Job.

# COMMAND ----------

recon = spark.sql(
    f"""
    WITH ordered AS (SELECT sku_key, count(*) AS units FROM workspace.silver.order_items GROUP BY sku_key),
    decremented AS (
      SELECT sku_key, sum(prev_stock_qty - stock_qty) AS units
      FROM {EVENTS} WHERE prev_stock_qty > stock_qty GROUP BY sku_key
    )
    SELECT count(*) AS skus,
           count_if(coalesce(o.units, 0) <> coalesce(d.units, 0)) AS mismatched,
           sum(o.units) AS ordered, sum(d.units) AS decremented
    FROM ordered o FULL OUTER JOIN decremented d USING (sku_key)
    """
).first()
print(recon.asDict())
assert (recon.skus, recon.mismatched, recon.ordered, recon.decremented) == (34_448, 0, 112_650, 112_650)
print("TIED OUT: every row and unit between raw Bronze and Silver is named; the sources agree on every SKU")