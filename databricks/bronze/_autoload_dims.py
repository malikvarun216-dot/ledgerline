# Databricks notebook source
# MAGIC %md
# MAGIC # Auto Loader → Bronze for nightly dumps — shared code
# MAGIC
# MAGIC Loaded with `%run ./_autoload_dims` by `autoload_dims` (production) and by Drill 1 (scratch
# MAGIC paths). **Defines functions only — running it reads nothing.**
# MAGIC
# MAGIC **Pattern:** Auto Loader + `Trigger.AvailableNow` + `replaceWhere` on `dump_date`.
# MAGIC
# MAGIC - **Auto Loader** remembers which files it has already read (in the checkpoint), so a normal
# MAGIC   re-run picks up only new nightly dumps.
# MAGIC - **`replaceWhere dump_date IN (...)`** makes the write itself idempotent: if a dump is processed
# MAGIC   again (checkpoint reset, re-delivery), its rows *replace* that night's rows instead of being
# MAGIC   appended a second time. The checkpoint prevents re-reads; `replaceWhere` makes a re-read
# MAGIC   harmless. Two independent guarantees.
# MAGIC - **`AvailableNow`**: process everything new, then stop. Serverless allows nothing else.
# MAGIC
# MAGIC Bronze is faithful: every column stays a string, exactly as the file had it, plus lineage.

# COMMAND ----------

from pyspark.sql import functions as F

PATH_DATE = r"dump_date=(\d{4}-\d{2}-\d{2})"

# Time travel (reading a table as it was at an older version) needs the old data files. Delta's
# log keeps entries 30 days by default, but data files only 7 — and Predictive Optimization, on
# for every table here, runs VACUUM that deletes them after that. 30 days lines the two up.
# decisions.md, "Predictive Optimization stays on" (Session 7).
FILE_RETENTION = "interval 30 days"


def ensure_retention(table):
    """Set the time-travel window once. Checked first, because every ALTER is a new commit."""
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    if props.get("delta.deletedFileRetentionDuration") != FILE_RETENTION:
        spark.sql(
            f"ALTER TABLE {table} SET TBLPROPERTIES "
            f"('delta.deletedFileRetentionDuration' = '{FILE_RETENTION}')"
        )
        print(f"{table}: old data files now kept {FILE_RETENTION}")


def write_batch(batch_df, batch_id, table):
    # The file carries dump_date as a column AND the folder is named dump_date=...
    # They must agree, or replaceWhere would replace the wrong night.
    mismatched = batch_df.where(F.col("dump_date") != F.col("_path_dump_date")).limit(1)
    if not mismatched.isEmpty():
        raise ValueError(f"{table}: dump_date column disagrees with its folder name")

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
    print(f"{table}: batch {batch_id} replaced dump_date {dates}")


def ingest(source, table, state, *, allow_overwrites=False):
    """One AvailableNow pass over `source`. State (schema + checkpoint) lives under `state`.

    `allow_overwrites`: whether a file rewritten in place (same path, newer modification time) is
    read again. Auto Loader's default is no — it tracks files by path, so a corrected re-delivery
    of a night is skipped without a word. Drill 1 tests exactly that.
    """
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        # Bronze keeps strings; typing is Silver's job.
        .option("cloudFiles.inferColumnTypes", "false")
        # Don't turn the dump_date=... folder into a column: the file already has one.
        .option("cloudFiles.partitionColumns", "")
        .option("cloudFiles.allowOverwrites", str(allow_overwrites).lower())
        .option("cloudFiles.schemaLocation", f"{state}/schema")
        .load(source)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_path_dump_date", F.regexp_extract("_source_file", PATH_DATE, 1))
    )

    # Create the empty table here, in the notebook's own session. Inside
    # foreachBatch (a cloned session on serverless) tableExists() returned False
    # for a table that existed — see incidents.md, 2026-09-26.
    if not spark.catalog.tableExists(table):
        empty = spark.createDataFrame([], stream.drop("_path_dump_date").schema)
        empty.write.format("delta").saveAsTable(table)
    ensure_retention(table)

    query = (
        stream.writeStream.foreachBatch(lambda df, bid: write_batch(df, bid, table))
        .option("checkpointLocation", f"{state}/checkpoint")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


def night_counts(table):
    """{dump_date: rows} as the table holds them now."""
    return {
        r.dump_date: r.n
        for r in spark.table(table).groupBy("dump_date").agg(F.count("*").alias("n")).collect()
    }


def replace_commits(table):
    """Every WRITE commit with its replaceWhere predicate and row count, oldest first."""
    return [
        (r.version, r.operationParameters.get("predicate"), r.operationMetrics.get("numOutputRows"))
        for r in spark.sql(f"DESCRIBE HISTORY {table}")
        .where("operation = 'WRITE'")
        .orderBy("version")
        .collect()
    ]
