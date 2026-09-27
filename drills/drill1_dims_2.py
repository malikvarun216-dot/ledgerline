# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Drill 1 — a corrected re-delivery, part 2 of 2 (scratch)
# MAGIC
# MAGIC **Run after** `drill1_dims_1` and `land_drill1_scratch.py redelivery-corrected`, which rewrote
# MAGIC night 2017-05-02 of the scratch seller dump **in place** (same path) with five corrected cities.
# MAGIC
# MAGIC Session 6 wrote that `replaceWhere` makes a re-read harmless "if the checkpoint is ever lost or
# MAGIC a night is backfilled". The checkpoint half was proven (and again in part 1). This tests the
# MAGIC backfill half:
# MAGIC
# MAGIC 1. **Default options.** Prediction: Auto Loader tracks files by path, the path is already in
# MAGIC    its checkpoint, so the corrected file is **skipped without a word** and Bronze keeps the old
# MAGIC    cities.
# MAGIC 2. **`cloudFiles.allowOverwrites = true`.** Auto Loader re-reads a file whose modification
# MAGIC    time changed. Normally that risks duplicates; here `replaceWhere` turns the re-read into
# MAGIC    "replace night 2017-05-02", so only that night changes and nothing doubles.

# COMMAND ----------

# MAGIC %run ../databricks/bronze/_autoload_dims

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "bronze"
TABLE = f"{CATALOG}.{SCHEMA}.drill1_seller"
SOURCE = "s3://ledgerline-landing-dev-fffc8b65/ledgerline/drill1/redelivery/seller/"
STATE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/drill1/redelivery"
NIGHT = "2017-05-02"
OTHER = "2017-01-02"
SUFFIX = " (corrected)"


def snapshot():
    counts = night_counts(TABLE)
    corrected = spark.table(TABLE).where(F.col("seller_city").endswith(SUFFIX)).count()
    other = spark.table(TABLE).where(F.col("dump_date") == OTHER)
    other_ingested = other.agg(F.max("_ingested_at")).first()[0]
    return counts, corrected, other_ingested, len(replace_commits(TABLE))


START = snapshot()
print(f"start: counts {START[0]}, corrected rows {START[1]}, WRITE commits {START[3]}")
assert START[1] == 0, "the table already has corrected rows — re-run drill1_dims_1 section 3 first"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Default options — the corrected file is already "known"

# COMMAND ----------

ingest(SOURCE, TABLE, STATE)
DEFAULT = snapshot()
print(f"after default run: counts {DEFAULT[0]}, corrected rows {DEFAULT[1]}, WRITE commits {DEFAULT[3]}")
assert DEFAULT[3] == START[3], "expected no write at all"
assert DEFAULT[1] == 0, "expected the correction NOT to land"
print("BUG PRESENT: a corrected re-delivery at the same path was skipped. No error, no write, old data kept.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `allowOverwrites = true` — the re-read lands, and `replaceWhere` keeps it to one night

# COMMAND ----------

ingest(SOURCE, TABLE, STATE, allow_overwrites=True)
FIXED = snapshot()
writes = replace_commits(TABLE)[START[3] :]
print(f"after allowOverwrites: counts {FIXED[0]}, corrected rows {FIXED[1]}, new writes {writes}")
assert FIXED[0] == START[0], "row counts must not move — the night is replaced, not appended"
assert FIXED[1] == 5, "expected the five corrected cities"
assert len(writes) == 1 and NIGHT in writes[0][1] and OTHER not in writes[0][1], "only 2017-05-02 replaced"
assert FIXED[2] == START[2], f"night {OTHER} was rewritten too"
display(
    spark.table(TABLE)
    .where(F.col("seller_city").endswith(SUFFIX))
    .select("seller_id", "seller_city", "dump_date", "_source_modified_at", "_ingested_at")
)
print("FIXED: the corrected night replaced itself; the other night and every count unchanged.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. And a plain re-run after that writes nothing

# COMMAND ----------

ingest(SOURCE, TABLE, STATE, allow_overwrites=True)
assert snapshot()[3] == FIXED[3], "a re-run with nothing changed wrote something"
display(
    spark.sql(f"DESCRIBE HISTORY {TABLE}")
    .select("version", "timestamp", "operation", "operationParameters", "operationMetrics.numOutputRows")
    .orderBy("version")
)