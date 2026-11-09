# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Repair: the one bridge file that still holds unredacted review text (incidents.md 2026-11-06)
# MAGIC
# MAGIC S22 redacted review text in Silver and in Snowflake's RAW; S28 repaired Databricks' raw. The
# MAGIC Parquet file both raws were loaded from still holds the 8 titles. This notebook repairs **the file**
# MAGIC — the earliest copy — without anyone loading it twice (decisions.md S29):
# MAGIC
# MAGIC 1. `plan` — reads only: the file, how many rows match Silver's `REDACT` pattern
# MAGIC    (expected 8 of 101,640).
# MAGIC 2. `copy` — writes a redacted copy to the same path under `repaired/` (Snowflake's stage and role
# MAGIC    never read it), same file name, rows in the same order; checks it; logs it in `repair_log`.
# MAGIC 3. `delete` — checks again, then deletes the original folder. **Last, and only after a green check.**
# MAGIC
# MAGIC Every mode runs **the check**: copy vs original row by row (only the two text columns may differ,
# MAGIC and exactly by `regexp_replace(REDACT)`), copy has 0 matches, and copy vs `workspace.raw` — the
# MAGIC repaired raw rows must be exactly what a load of the copy would give (by file name and row number).
# MAGIC
# MAGIC `plan` writes no data; it only creates the empty `repair_log` table if it is missing.
# MAGIC
# MAGIC Run by the owner (writes need the `ledgerline_bridge` external location). The check reads
# MAGIC `workspace.raw.silver_changes`, which `ledgerline-ci` owns: the owner needs `SELECT` on it, granted
# MAGIC as the schema's owner (S29: `GRANT SELECT ON TABLE workspace.raw.silver_changes TO <you>`).
# MAGIC After `delete`: run
# MAGIC `bridge/export_silver` in `plan` mode — Verify 1 and 2 must stay green.

# COMMAND ----------

# MAGIC %run ../bridge/_export_silver

# COMMAND ----------

# MAGIC %run ../silver/_merge_dims

# COMMAND ----------

import posixpath

dbutils.widgets.dropdown("mode", "plan", ["plan", "copy", "delete"])
MODE = dbutils.widgets.get("mode")

TABLE = "review"
RANGE = "v0000000000-0000000007"
ORIGINAL = f"{BRIDGE_ROOT}/{TABLE}/{RANGE}"
COPY = f"{REPAIRED_ROOT}/{TABLE}/{RANGE}"
TEXT = ["review_comment_title", "review_comment_message"]
RAW = "workspace.raw.silver_changes"
BUCKET = BRIDGE_ROOT.rsplit("/", 1)[0] + "/"  # s3://ledgerline-bridge-8176114c/
REASON = "S22 redaction (8+ digit runs, emails) missing from the exported file; incidents.md 2026-11-06"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {REPAIR_LOG} (
  table_name          STRING    COMMENT 'Silver table of the exported file',
  original_path       STRING    COMMENT 'the file as exported, under silver/',
  repaired_path       STRING    COMMENT 'its replacement, under repaired/silver/, same file name',
  rows                BIGINT    COMMENT 'rows in both files (equal)',
  rows_changed        BIGINT    COMMENT 'rows whose text the repair changed',
  reason              STRING,
  repaired_at         TIMESTAMP COMMENT 'when the copy was written and checked',
  original_deleted_at TIMESTAMP COMMENT 'when the original was deleted; NULL = repair half done'
)
COMMENT 'Bridge files replaced after export (decisions.md S29); export_log says what was exported'
""")


def parquet_files(path):
    try:
        return sorted(f.path for f in dbutils.fs.ls(path) if f.name.endswith(".parquet"))
    except Exception as err:  # only "missing" is expected
        if _missing_path(err):
            return []
        raise


def with_row(path):
    """The file's rows with their row number from 1 — what dbx_raw stores as source_row."""
    return spark.read.parquet(path).select("*", (F.col("_metadata.row_index") + 1).alias("_row"))


def matches(df):
    return df.where(" OR ".join(f"{c} rlike '{REDACT}'" for c in TEXT)).count()


def check_copy_vs_original(original_file, copy_file):
    o, c = with_row(original_file), with_row(copy_file)
    cols = [x for x in o.columns if x != "_row"]
    joined = o.alias("o").join(c.alias("c"), "_row", "full_outer")
    same = [
        (F.col(f"c.{x}").eqNullSafe(F.regexp_replace(F.col(f"o.{x}"), REDACT, REDACTED)) if x in TEXT
         else F.col(f"c.{x}").eqNullSafe(F.col(f"o.{x}")))
        for x in cols
    ]
    all_same = same[0]
    for s in same[1:]:
        all_same = all_same & s
    text_changed = None
    for x in TEXT:
        d = ~F.col(f"c.{x}").eqNullSafe(F.col(f"o.{x}"))
        text_changed = d if text_changed is None else text_changed | d
    return {
        "same_name": posixpath.basename(original_file) == posixpath.basename(copy_file),
        "same_schema": o.schema == c.schema,
        "rows_original": o.count(),
        "rows_copy": c.count(),
        "rows_wrong": joined.where(~all_same).count(),
        "rows_changed": joined.where(text_changed).count(),
        "matches_in_copy": matches(c),
    }


def check_copy_vs_raw(copy_file):
    """The repaired raw rows vs a load of the copy, by (file name, row number), as JSON text
    (a VARIANT has no `=`). Both 0 = a raw rebuilt from the copy equals the raw we have."""
    c = with_row(copy_file)
    cols = ", ".join(f"`{x}`" for x in c.columns if x != "_row")
    loaded = c.select(F.col("_row").alias("source_row"),
                      F.expr(f"to_json(to_variant_object(struct({cols})))").alias("j"))
    source = copy_file.replace(BUCKET + "repaired/", "")  # the original's path, as raw stores it
    held = (spark.table(RAW).where(F.col("source_file") == source)
            .select("source_row", F.expr("to_json(record)").alias("j")))
    return {
        "raw_rows": held.count(),
        "only_in_raw": held.exceptAll(loaded).count(),
        "only_in_copy": loaded.exceptAll(held).count(),
    }


def check(original_file, copy_file):
    result = {}
    if original_file:
        result |= check_copy_vs_original(original_file, copy_file)
    result |= check_copy_vs_raw(copy_file)
    print(result)
    if original_file:
        assert result["same_name"] and result["same_schema"], "copy is not the original's twin"
        assert result["rows_original"] == result["rows_copy"], "row counts differ"
        assert result["rows_wrong"] == 0, "a row differs beyond the redaction"
    assert result["only_in_raw"] == result["only_in_copy"] == 0, "raw and the copy disagree"
    assert result.get("matches_in_copy", matches(with_row(copy_file))) == 0, "the copy still holds matches"
    return result


originals, copies = parquet_files(ORIGINAL), parquet_files(COPY)
logged = spark.table(REPAIR_LOG).where(F.col("repaired_path") == (copies[0] if copies else ""))
print(f"original: {originals}\ncopy:     {copies}\nlog lines for the copy: {logged.count()}\nmode = {MODE}")
assert len(originals) <= 1 and len(copies) <= 1, "expected one file per folder"
# Everything the final check reads, read once before any mode writes (incidents.md 2026-11-06, S29):
# a missing grant fails here, while nothing has changed.
print(f"raw holds {spark.table(RAW).where(F.col('table_name') == TABLE).count():,} {TABLE} rows")
if originals:
    o = with_row(originals[0])
    print(f"original: {o.count():,} rows, {matches(o)} match REDACT")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `plan` — check what exists, write nothing

# COMMAND ----------

if MODE == "plan":
    if copies:
        check(originals[0] if originals else None, copies[0])
    print("plan only — nothing written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `copy` — the redacted twin, checked and logged

# COMMAND ----------

if MODE == "copy":
    assert originals, "copy needs the original"
    target = f"{COPY}/{posixpath.basename(originals[0])}"  # the original's name: raw keys on it
    if copies:
        # A copy with no log line: an earlier copy run died after writing it (S29: the check could not
        # read raw). It is checked now like a fresh one; only the write is skipped.
        assert copies == [target] and logged.count() == 0, "a copy exists that this run did not expect"
        print(f"resuming: {target} exists with no log line — checking it")
    else:
        src = with_row(originals[0])
        redacted = src.select(
            *[F.regexp_replace(F.col(x), REDACT, REDACTED).alias(x) if x in TEXT else F.col(x)
              for x in src.columns],
        )
        # One partition, sorted by the original row number: the copy's row N is the original's row N.
        redacted.repartition(1).sortWithinPartitions("_row").drop("_row").write.mode("error").parquet(COPY)
        written = parquet_files(COPY)
        assert len(written) == 1, written
        dbutils.fs.mv(written[0], target)
    result = check(originals[0], target)
    spark.createDataFrame(
        [(TABLE, originals[0], target, result["rows_copy"], result["rows_changed"], REASON)],
        "table_name STRING, original_path STRING, repaired_path STRING, rows BIGINT, rows_changed BIGINT, "
        "reason STRING",
    ).withColumn("repaired_at", F.current_timestamp()).withColumn(
        "original_deleted_at", F.lit(None).cast("timestamp")
    ).write.mode("append").saveAsTable(REPAIR_LOG)
    print(f"copied and checked: {target}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `delete` — check once more, then delete the original folder

# COMMAND ----------

if MODE == "delete":
    assert originals and copies and logged.count() == 1, "delete needs the original, the copy, its log line"
    check(originals[0], copies[0])
    dbutils.fs.rm(ORIGINAL, True)
    assert parquet_files(ORIGINAL) == [], "the original is still there"
    spark.sql(f"UPDATE {REPAIR_LOG} SET original_deleted_at = current_timestamp() "
              f"WHERE repaired_path = '{copies[0]}'")
    check(None, copies[0])
    print(f"deleted {ORIGINAL}; now run bridge/export_silver in plan mode (Verify 1 and 2)")