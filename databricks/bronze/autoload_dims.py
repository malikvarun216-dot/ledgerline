# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — nightly dimension dumps (customer, product, seller)
# MAGIC
# MAGIC **Pattern:** Auto Loader + `Trigger.AvailableNow` + `replaceWhere` on `dump_date`.
# MAGIC
# MAGIC - **Auto Loader** remembers which files it has already read (in the checkpoint), so a normal
# MAGIC   re-run picks up only new nightly dumps.
# MAGIC - **`replaceWhere dump_date IN (...)`** makes the write itself idempotent: if a dump is ever
# MAGIC   processed again (checkpoint reset, backfill), its rows *replace* that night's rows instead of
# MAGIC   being appended a second time. The checkpoint prevents re-reads; `replaceWhere` makes a
# MAGIC   re-read harmless. Two independent guarantees.
# MAGIC - **`AvailableNow`**: process everything new, then stop. Serverless allows nothing else.
# MAGIC
# MAGIC Bronze is faithful: every column stays a string, exactly as the file had it, plus lineage.
# MAGIC Source is read-only (`ledgerline_landing` external location); state lives in a UC volume.

# COMMAND ----------

from pyspark.sql import functions as F

LANDING ="s3://ledgerline-landing-dev-fffc8b65/ledgerline/dims"
CATALOG = "workspace"
SCHEMA = "bronze"
STATE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/dims"
DIMS = ["customer", "product", "seller"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

# COMMAND ----------

PATH_DATE = r"dump_date=(\d{4}-\d{2}-\d{2})"


def write_batch(batch_df, batch_id, dim):
    table = f"{CATALOG}.{SCHEMA}.{dim}"

    # The file carries dump_date as a column AND the folder is named dump_date=...
    # They must agree, or replaceWhere would replace the wrong night.
    mismatched = batch_df.where(F.col("dump_date") != F.col("_path_dump_date")).limit(1)
    if not mismatched.isEmpty():
        raise ValueError(f"{dim}: dump_date column disagrees with its folder name")

    dates = sorted(r.dump_date for r in batch_df.select("dump_date").distinct().collect())
    if not dates:
        return
    rows = batch_df.drop("_path_dump_date")

    # replaceWhere is a predicate, not a partition operation: the table is not
    # partitioned (~100K rows would be thousands of tiny files). Delta also
    # checks every written row matches the predicate, so a stray date fails loudly.
    #
    # Safe only because one night = one file = one batch. If a night's dump ever
    # arrived as several files across two batches, the second batch would
    # replace the first one's rows. The replace unit must equal the arrival unit.
    #
    # One path only: the table is created before the stream starts (see ingest),
    # so every batch, including the first ever, goes through replaceWhere.
    in_list = ", ".join(f"'{d}'" for d in dates)
    (
        rows.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"dump_date IN ({in_list})")
        .saveAsTable(table)
    )
    print(f"{dim}: batch {batch_id} replaced dump_date {dates}")


def ingest(dim):
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        # Bronze keeps strings; typing is Silver's job.
        .option("cloudFiles.inferColumnTypes", "false")
        # Don't turn the dump_date=... folder into a column: the file already has one.
        .option("cloudFiles.partitionColumns", "")
        .option("cloudFiles.schemaLocation", f"{STATE}/{dim}/schema")
        .load(f"{LANDING}/{dim}/")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_path_dump_date", F.regexp_extract("_source_file", PATH_DATE, 1))
    )

    # Create the empty table here, in the notebook's own session. Inside
    # foreachBatch (a cloned session on serverless) tableExists() returned False
    # for a table that existed — see incidents.md, 2026-09-26.
    table = f"{CATALOG}.{SCHEMA}.{dim}"
    if not spark.catalog.tableExists(table):
        empty = spark.createDataFrame([], stream.drop("_path_dump_date").schema)
        empty.write.format("delta").saveAsTable(table)

    query = (
        stream.writeStream
        .foreachBatch(lambda df, bid: write_batch(df, bid, dim))
        .option("checkpointLocation", f"{STATE}/{dim}/checkpoint")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


for dim in DIMS:
    ingest(dim)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — counts must equal the files, parsed independently on a laptop

# COMMAND ----------

EXPECTED = {
    "customer": {"2016-09-04": 1, "2017-01-02": 326, "2017-05-02": 8068},
    "product": {"2016-09-04": 32951, "2017-01-02": 32951, "2017-05-02": 32951},
    "seller": {"2016-09-04": 3095, "2017-01-02": 3095, "2017-05-02": 3095},
}


def check_counts():
    for dim, want in EXPECTED.items():
        got = {
            r.dump_date: r.n
            for r in spark.table(f"{CATALOG}.{SCHEMA}.{dim}")
            .groupBy("dump_date").agg(F.count("*").alias("n")).collect()
        }
        status = "OK" if got == want else "MISMATCH"
        print(f"{dim:9} {status}  got={dict(sorted(got.items()))}")
        assert got == want, f"{dim}: expected {want}"


check_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prove idempotency — twice, two different ways
# MAGIC
# MAGIC 1. **Plain re-run.** No new files, so Auto Loader has nothing to do. Counts unchanged.
# MAGIC 2. **Forced replay.** Delete the checkpoints, so Auto Loader forgets everything and re-reads
# MAGIC    all 9 files. Without `replaceWhere` every table would double. With it, counts stay the same.

# COMMAND ----------

for dim in DIMS:
    ingest(dim)
check_counts()

# COMMAND ----------

dbutils.fs.rm(STATE, True)
for dim in DIMS:
    ingest(dim)
check_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look at what happened — each write is its own commit

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.customer")
    .select("version", "timestamp", "operation", "operationParameters", "operationMetrics")
)
