# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver → Snowflake bridge — shared code
# MAGIC
# MAGIC Loaded with `%run ./_export_silver` by `export_silver`. **Defines functions only — running it
# MAGIC writes nothing.**
# MAGIC
# MAGIC **What crosses the bridge:** each Silver table's **Change Data Feed** — one row per row a commit
# MAGIC inserted, updated (its new values, `update_postimage`) or **deleted** — with the Delta version it
# MAGIC came from (`_commit_version`) and that commit's time (`_commit_timestamp`). Not the tables
# MAGIC themselves: Snowflake gets *what changed since version N*, and rebuilds current state (and, for the
# MAGIC dims, history) from it. `update_preimage` rows (the old values) stay behind — the previous change
# MAGIC for the same key already carries them.
# MAGIC
# MAGIC **One export = one table, one version range, one folder, written once:**
# MAGIC `s3://ledgerline-bridge-8176114c/silver/<table>/v<from>-<to>/part-….parquet`, then a line in
# MAGIC `workspace.bridge.export_log`. The next export starts at the last line's `to + 1`.
# MAGIC
# MAGIC - **Nothing committed since the last export:** nothing written, nothing logged.
# MAGIC - **Commits that changed no row** (OPTIMIZE, a constraint): range logged, 0 rows, no folder.
# MAGIC - **A folder in the bucket with no log line** (a run died between the write and the log):
# MAGIC   **refused** — a person decides, because Snowpipe may already have loaded it.
# MAGIC - **A range whose folder already exists:** the write fails (`errorifexists`) — never overwritten.
# MAGIC - **A rename / drop column inside the range:** the change feed refuses
# MAGIC   (`DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE`, S11 E5) — a new baseline, decided by a person.
# MAGIC
# MAGIC **At-least-once, on purpose.** If a run dies after the write and before the log, the folder
# MAGIC exists with no line; the refusal makes a person look. Downstream, Gold applies changes by
# MAGIC `(key, _commit_version)`, so a range loaded twice changes nothing (decisions.md, Session 13).

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

SILVER = "workspace.silver"
BRIDGE_SCHEMA = "workspace.bridge"
EXPORT_LOG = f"{BRIDGE_SCHEMA}.export_log"
BRIDGE_ROOT = "s3://ledgerline-bridge-8176114c/silver"

# Every Silver data table with a change feed, and the columns that identify one row. PII is not here:
# it lives in workspace.pii, and how it reaches Snowflake is decided in Session 18.
TABLES = {
    "customer": ["customer_unique_id"],
    "product": ["product_id"],
    "seller": ["seller_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "inventory": ["sku_key"],
    "inventory_events": ["event_id"],
}
EXPORTED_CHANGES = ("insert", "update_postimage", "delete")
META = ["_change_type", "_commit_version", "_commit_timestamp"]

# One clock with Silver and Snowflake (UTC), and timestamps written as plain microseconds since 1970.
# Spark's default Parquet timestamp is the legacy INT96 type; TIMESTAMP_MICROS is the standard one
# every reader agrees on.
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")


class BridgeRefused(Exception):
    """The bucket and the export log disagree. Nothing was written."""

# COMMAND ----------


def create_log():
    """In the notebook's own session, before any export."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRIDGE_SCHEMA}")
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {EXPORT_LOG} (
              table_name STRING, from_version BIGINT, to_version BIGINT, path STRING,
              inserts BIGINT, updates BIGINT, deletes BIGINT, rows BIGINT, files INT,
              exported_at TIMESTAMP)"""
    )


def _missing_path(err):
    """dbutils says "not found", "FileNotFound" or "No such file", depending on the store (S7)."""
    text = str(err).lower()
    return any(s in text for s in ("not found", "filenotfound", "no such file"))


def folder(name, start, end):
    """Zero-padded, so the folder names sort in version order."""
    return f"{BRIDGE_ROOT}/{name}/v{start:010d}-{end:010d}"


def existing_folders(name):
    try:
        return sorted(f.path.rstrip("/") for f in dbutils.fs.ls(f"{BRIDGE_ROOT}/{name}/") if f.isDir())
    except Exception as err:  # only "missing" is expected; anything else is re-raised
        if _missing_path(err):
            return []
        raise


def current_version(table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first().version


def plan(name):
    """What the next export of `name` would do. Reads only."""
    lines = spark.table(EXPORT_LOG).where(F.col("table_name") == name)
    last = lines.agg(F.max("to_version").alias("v")).first().v
    logged = {r.path for r in lines.where("path IS NOT NULL").select("path").collect()}
    return {
        "table": name,
        "from": 0 if last is None else last + 1,
        "to": current_version(f"{SILVER}.{name}"),
        "orphans": [p for p in existing_folders(name) if p not in logged],
    }


def changes(name, start, end):
    """The change feed for a FIXED version range: the same rows every time it is read, so counting
    and writing it are two reads of one thing (the Drill 2 'recipe' trap needs a moving source)."""
    return (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", start)
        .option("endingVersion", end)
        .table(f"{SILVER}.{name}")
        .where(F.col("_change_type").isin(*EXPORTED_CHANGES))
    )


def export(name):
    """Export everything `name` committed since its last export. Returns the log line, or None."""
    p = plan(name)
    if p["orphans"]:
        raise BridgeRefused(
            f"{name}: {len(p['orphans'])} folder(s) in the bucket with no export_log line: {p['orphans']}. "
            "A run died between writing and logging. Check Snowflake's load history for these files "
            "before deciding: never loaded -> delete the folder; loaded -> add its log line."
        )
    start, end = p["from"], p["to"]
    if start > end:
        return None

    rows = changes(name, start, end)
    counts = {
        r._change_type: r.n
        for r in rows.groupBy("_change_type").agg(F.count(F.lit(1)).alias("n")).collect()
    }
    total = sum(counts.values())
    path, files = None, 0
    if total:
        path = folder(name, start, end)
        # One file per range: small enough here (the biggest history is a few hundred thousand rows),
        # and one file = one entry in Snowflake's load history, which is what Session 13 tests.
        rows.coalesce(1).write.mode("errorifexists").parquet(path)
        files = sum(1 for f in dbutils.fs.ls(path) if f.name.endswith(".parquet"))

    line = spark.createDataFrame(
        [(name, start, end, path, counts.get("insert", 0), counts.get("update_postimage", 0),
          counts.get("delete", 0), total, files)],
        "table_name STRING, from_version BIGINT, to_version BIGINT, path STRING, inserts BIGINT, "
        "updates BIGINT, deletes BIGINT, rows BIGINT, files INT",
    ).withColumn("exported_at", F.current_timestamp())
    line.write.mode("append").saveAsTable(EXPORT_LOG)
    return line.first().asDict()

# COMMAND ----------


def replay(name):
    """Silver's current rows, rebuilt from the bridge files alone: each key's newest change, kept
    unless that change is a delete. This is what Gold's MERGE will do in Snowflake."""
    keys = TABLES[name]
    exported = spark.read.option("recursiveFileLookup", "true").parquet(f"{BRIDGE_ROOT}/{name}/")
    newest = Window.partitionBy(*keys).orderBy(F.col("_commit_version").desc())
    return (
        exported.withColumn("_n", F.row_number().over(newest))
        .where("_n = 1 AND _change_type <> 'delete'")
        .drop("_n", *META)
    )


def compare_with_silver(name):
    """Rows in the replay but not in Silver, and the reverse. Both 0 = the files rebuild Silver.
    Compared with Silver AS OF the last exported version, so a Silver run after the export does not
    show up as a difference."""
    version = spark.table(EXPORT_LOG).where(F.col("table_name") == name).agg(F.max("to_version")).first()[0]
    silver = spark.read.option("versionAsOf", version).table(f"{SILVER}.{name}")
    rebuilt = replay(name).select(*silver.columns)
    return {
        "version": version,
        "silver_rows": silver.count(),
        "rebuilt_rows": rebuilt.count(),
        "only_in_rebuilt": rebuilt.exceptAll(silver).count(),
        "only_in_silver": silver.exceptAll(rebuilt).count(),
    }