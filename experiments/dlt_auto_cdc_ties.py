# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Deliberate — AUTO CDC fed two different events at one `(sku_key, seq)`
# MAGIC
# MAGIC Bronze `inventory_cdc` still holds the Session 6 contamination: **53 events** (159 copies) that share
# MAGIC a SKU and a `seq` with a real event but carry other values. Production Silver **refuses** such a
# MAGIC batch before its MERGE (`find_problems` → `tied_seq_vs_log`), and the denylist removes them. The
# MAGIC Lakeflow pipeline `ledgerline-dlt` drops them by an expectation. This experiment turns that off and
# MAGIC asks: **what does AUTO CDC do with an ordering it cannot decide?** Lakeflow's documentation states
# MAGIC "one distinct update per key at each sequencing value" as a requirement; whether it is *checked* is
# MAGIC what this measures.
# MAGIC
# MAGIC **How to run** (Session 11; only the pipeline's own schema `workspace.silver_dlt` changes):
# MAGIC 1. Pipeline settings → Configuration: `ledgerline.apply_denylist` = `false`.
# MAGIC 2. Run the pipeline with a **full refresh** of the three inventory tables.
# MAGIC 3. Run this notebook. It writes nothing.
# MAGIC 4. Restore: remove the setting, full refresh the same three tables, then run
# MAGIC    `databricks/checks/dlt_vs_silver` (all green again).
# MAGIC
# MAGIC **Prediction, written before the run:** no error and no warning; one SCD2 version per `(sku_key,
# MAGIC seq)` (158,346 + 0 new starts — every contaminated event starts where a real one does); at each of
# MAGIC the 53 starts AUTO CDC keeps **one** of the two events, which one not decided by anything we wrote;
# MAGIC SCD1 unchanged (exp_04 D: no contaminated event is a SKU's newest).

# COMMAND ----------

# MAGIC %run ../databricks/silver/_merge_inventory_cdc

# COMMAND ----------

DLT = "workspace.silver_dlt"
CHECKED = f"{DLT}.inventory_cdc_checked"
SCD1 = f"{DLT}.inventory_scd1"
SCD2 = f"{DLT}.inventory_scd2"

DENYLIST = load_denylist()
spark.createDataFrame([(e,) for e in DENYLIST], "event_id string").createOrReplaceTempView("denied_ids")

pre = spark.sql(f"""
    SELECT count(*) AS rows, count(d.event_id) AS denied_rows, count(DISTINCT d.event_id) AS denied_events
    FROM {CHECKED} c LEFT JOIN denied_ids d USING (event_id)
""").first()
print(f"inventory_cdc_checked: {pre.asDict()}")
assert pre.denied_rows == 159, (
    "the pipeline still drops the denylist — set ledgerline.apply_denylist = false and full refresh first"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — at each tied `(sku_key, seq)`: which event did SCD2 keep?

# COMMAND ----------

ties = spark.sql(f"""
    WITH denied AS (
        SELECT DISTINCT c.event_id, c.sku_key, c.seq, c.op, c.stock_qty
        FROM {CHECKED} c JOIN denied_ids USING (event_id)
    ),
    pairs AS (
        SELECT d.sku_key, d.seq, d.op,
               d.event_id AS bad_event,  d.stock_qty AS bad_stock,
               r.event_id AS real_event, r.stock_qty AS real_stock
        FROM denied d LEFT JOIN {EVENTS} r ON r.sku_key = d.sku_key AND r.seq = d.seq
    )
    SELECT p.*, v.event_id AS kept_event, v.stock_qty AS kept_stock,
           CASE WHEN v.sku_key IS NULL        THEN 'no version'
                WHEN v.event_id = p.real_event THEN 'real'
                WHEN v.event_id = p.bad_event  THEN 'contaminated'
                ELSE 'other' END AS kept
    FROM pairs p LEFT JOIN {SCD2} v ON v.sku_key = p.sku_key AND v.__START_AT = p.seq
""")
# 53 rows: collected once, so every count below reads the same rows. `.cache()` is refused on
# serverless (`NOT_SUPPORTED_WITH_SERVERLESS`: PERSIST TABLE), seen the first time this ran.
ties = spark.createDataFrame(ties.collect(), ties.schema)
summary = ties.agg(
    F.count(F.lit(1)).alias("rows"),
    F.count_if(F.col("real_event").isNull()).alias("no_real_twin"),
    F.count_if(F.col("bad_stock") != F.col("real_stock")).alias("stock_differs"),
    F.count_if(F.col("kept") == "real").alias("kept_real"),
    F.count_if(F.col("kept") == "contaminated").alias("kept_contaminated"),
    F.count_if(F.col("kept").isin("no version", "other")).alias("neither"),
).first()
print(f"53 ties: {summary.asDict()}")
display(ties.orderBy("sku_key", "seq"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — the tables as a whole

# COMMAND ----------

versions = spark.sql(f"""
    SELECT count(*) AS versions, count_if(__END_AT IS NULL) AS open,
           count(DISTINCT sku_key, __START_AT) AS distinct_starts
    FROM {SCD2}
""").first()
scd1_vs_silver = spark.sql(f"""
    SELECT count(*) AS skus,
           count_if(p.sku_key IS NULL OR d.sku_key IS NULL
                    OR p.stock_qty <> d.stock_qty OR p.seq <> d.seq) AS different
    FROM {STOCK} p FULL OUTER JOIN {SCD1} d ON p.sku_key = d.sku_key
""").first()
print(f"SCD2: {versions.asDict()}  (clean run: 158,346 versions, 34,348 open)")
print(f"SCD1 vs production silver.inventory: {scd1_vs_silver.asDict()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — did the pipeline say anything?
# MAGIC
# MAGIC Every warning and error the pipeline logged, newest first. Production's answer to this batch is a
# MAGIC refusal and a failure email; this is where AUTO CDC's answer would be.

# COMMAND ----------

display(spark.sql(f"""
    SELECT timestamp, level, event_type, origin.flow_name, message
    FROM event_log(TABLE({SCD2}))
    WHERE level IN ('WARN', 'ERROR')
    ORDER BY timestamp DESC
    LIMIT 50
"""))