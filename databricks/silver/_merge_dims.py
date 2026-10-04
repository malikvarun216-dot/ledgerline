# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bronze → Silver for the nightly dimension dumps — shared code
# MAGIC
# MAGIC Loaded with `%run ./_merge_dims` by `merge_dims` (production) and by `exp_03` (scratch tables).
# MAGIC **Defines functions only — running it writes nothing.**
# MAGIC
# MAGIC **Bronze** holds every night's full dump side by side. **Silver** holds one row per customer,
# MAGIC product and seller *as it is now*. It gets there by applying each night, oldest first, with one
# MAGIC `MERGE` — a single statement that inserts, updates and deletes in one commit:
# MAGIC
# MAGIC | in tonight's file? | in Silver? | clause | result |
# MAGIC |---|---|---|---|
# MAGIC | yes | no | `WHEN NOT MATCHED` | insert |
# MAGIC | yes, a value differs | yes | `WHEN MATCHED AND NOT (all equal)` | update |
# MAGIC | yes, identical | yes | — | nothing; the row is not rewritten |
# MAGIC | **no** | yes | `WHEN NOT MATCHED BY SOURCE` | **delete** |
# MAGIC
# MAGIC The last row is right **only because every file is a full snapshot**: absent means gone. Three
# MAGIC guards follow from that one fact (decisions.md, Session 8):
# MAGIC 1. a truncated file would delete real rows → **delete circuit breaker**, at most 5% per night;
# MAGIC 2. setting a bad row aside would delete it from Silver → a bad row is **held, not removed**: it
# MAGIC    stays in the MERGE source flagged `_ok = false`, so its key is present (no delete) but it may
# MAGIC    not update or insert. Copied to `silver.dims_rejected_rows`. **Filter the changes, never the
# MAGIC    keys.** (Session 9; Session 8 refused the whole night instead.)
# MAGIC 3. a file that looks broken — more than 1% bad rows, a NULL or duplicate key, no rows — is
# MAGIC    **refused whole**; a duplicate key is never resolved by picking one (the CDC path is the
# MAGIC    opposite: several rows per key are normal there).

# COMMAND ----------

from pyspark.sql import functions as F

BRONZE = "workspace.bronze"
SILVER = "workspace.silver"
MERGE_LOG = f"{SILVER}.dims_merge_log"
REJECTED = f"{SILVER}.dims_rejected_rows"
MAX_DELETE_FRACTION = 0.05
# Above this share of bad rows the file itself is broken (a shifted column, a wrong delimiter), not a
# few rows in it: the whole night is refused. Same shape as Snowflake's ON_ERROR = SKIP_FILE_<n>%.
MAX_BAD_FRACTION = 0.01

# Change Data Feed records which rows each commit changed. It only records from the moment it is
# on, so it is set at creation. Old files kept 30 days (Session 7's rule) — the change feed's own
# files are cleaned on the same clock, so whatever reads the feed must read within 30 days.
TABLE_PROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
}

# Text like "2017-03-10 21:05:03" has no time zone; casting it to TIMESTAMP uses the session's.
# Pinned, so the same text always becomes the same instant. If the zone moved between two nights,
# every row's dim_updated_at would "differ" and the MERGE would rewrite every row.
spark.conf.set("spark.sql.session.timeZone", "UTC")

# How each Bronze string becomes a Silver value. Bronze keeps the text exactly as delivered.
#   zip        five-digit text; the dumps lost the leading zero (incidents.md 2026-09-30)
#   int        whole number
#   timestamp  a date, or a date and time, with no zone
#   string     kept as it is
DIMS = {
    "customer": {
        "key": "customer_unique_id",
        "columns": {
            "customer_zip_code_prefix": "zip",
            "customer_city": "string",
            "customer_state": "string",
            "dim_updated_at": "timestamp",
        },
    },
    "product": {
        "key": "product_id",
        "columns": {
            "product_category_name": "string",
            "product_weight_g": "int",
            "product_length_cm": "int",
            "product_height_cm": "int",
            "product_width_cm": "int",
            "product_photos_qty": "int",
            "dim_updated_at": "timestamp",
        },
    },
    "seller": {
        "key": "seller_id",
        "columns": {
            "seller_zip_code_prefix": "zip",
            "seller_city": "string",
            "seller_state": "string",
            "dim_updated_at": "timestamp",
        },
    },
}
REQUIRED = {"dim_updated_at"}  # never NULL, besides the key
SQL_TYPE = {"zip": "STRING", "int": "INT", "timestamp": "TIMESTAMP", "string": "STRING"}
LINEAGE = ["_first_seen_dump_date", "_last_changed_dump_date", "_source_file", "_merged_at"]


class NightRefused(Exception):
    """A night's file looks broken as a whole. Silver keeps the previous night."""


class MassDeleteRefused(Exception):
    """A night would delete more of Silver than MAX_DELETE_FRACTION allows."""


class MergeLogLost(Exception):
    """Silver holds rows but the merge log has no line for that dimension (Drill 2)."""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Typing and the per-night contract

# COMMAND ----------


def typed_value(column, kind):
    """SQL turning the Bronze text in `column` into its Silver value. NULL stays NULL."""
    if kind == "zip":
        return f"lpad({column}, 5, '0')"
    if kind == "int":
        return f"try_cast({column} AS INT)"
    if kind == "timestamp":
        return f"try_cast({column} AS TIMESTAMP)"
    return column


def bad_value(column, kind):
    """SQL that is true when `column` holds text that cannot become its Silver type."""
    if kind == "zip":
        # Checked on the raw text: lpad also *cuts* anything longer than 5, silently.
        return f"({column} IS NOT NULL AND NOT {column} RLIKE '^[0-9]{{1,5}}$')"
    if kind in ("int", "timestamp"):
        # try_cast gives NULL instead of an error; text that was there but became NULL is bad.
        return f"({column} IS NOT NULL AND {typed_value(column, kind)} IS NULL)"
    return "false"


def unfit(column, kind):
    """SQL true when this column makes the row unfit to apply: a bad value, or a required one missing."""
    sql = bad_value(column, kind)
    return f"({sql} OR {column} IS NULL)" if column in REQUIRED else sql


def bad_row(dim):
    """SQL true when any column of the row is unfit. Such a row is HELD, never removed."""
    return " OR ".join(unfit(c, kind) for c, kind in DIMS[dim]["columns"].items())


def bronze_night(dim, night, bronze=BRONZE):
    return spark.table(f"{bronze}.{dim}").where(F.col("dump_date") == night)


def check_night(dim, raw):
    """Count, in one pass, the bad rows and every way the file as a whole looks broken.

    Returns (stats, problems). `problems` must be empty, or the whole night is refused: no rows, a NULL
    or duplicate key, or more than MAX_BAD_FRACTION bad rows. Fewer bad rows are `stats["held"]`:
    applied as "no change" for that key, never filtered out — in this MERGE a row missing from the
    source is a delete, so dropping a bad row deletes it (exp_03 D).
    """
    spec = DIMS[dim]
    key = spec["key"]
    aggs = [
        F.count(F.lit(1)).alias("rows"),
        F.count_distinct(key).alias("distinct_keys"),
        F.sum(F.col(key).isNull().cast("int")).alias("null_keys"),
        F.sum(F.expr(bad_row(dim)).cast("int")).alias("held"),
    ]
    for column, kind in spec["columns"].items():
        aggs.append(F.sum(F.expr(bad_value(column, kind)).cast("int")).alias(f"bad__{column}"))
        if column in REQUIRED:
            aggs.append(F.sum(F.col(column).isNull().cast("int")).alias(f"missing__{column}"))
        if kind == "zip":
            aggs.append(F.sum((F.length(column) < 5).cast("int")).alias(f"padded__{column}"))
    stats = {k: (v or 0) for k, v in raw.agg(*aggs).first().asDict().items()}  # sum of 0 rows is NULL

    problems = {}
    if stats["rows"] == 0:
        problems["empty_night"] = 0
    if stats["null_keys"]:
        problems["null_keys"] = stats["null_keys"]
    duplicates = stats["rows"] - stats["null_keys"] - stats["distinct_keys"]  # distinct skips NULL
    if duplicates:
        problems["duplicate_keys"] = duplicates
    if stats["rows"] and stats["held"] / stats["rows"] > MAX_BAD_FRACTION:
        problems["bad_rows"] = f"{stats['held']:,} of {stats['rows']:,} ({stats['held'] / stats['rows']:.1%})"
        problems.update({k: v for k, v in stats.items() if k.startswith(("bad__", "missing__")) and v})
    stats["padded_zips"] = sum(v for k, v in stats.items() if k.startswith("padded__"))
    return stats, problems


def typed_night(dim, raw):
    """The night as Silver will hold it: key, typed columns, where it came from, and `_ok` — false for a
    held row, which stays in the source (its key protects it from the delete) but changes nothing."""
    spec = DIMS[dim]
    return raw.select(
        F.col(spec["key"]),
        *[F.expr(typed_value(c, kind)).alias(c) for c, kind in spec["columns"].items()],
        F.to_date("dump_date").alias("_dump_date"),
        F.col("_source_file"),
        (~F.expr(bad_row(dim))).alias("_ok"),
    )


def rejected_rows(dim, raw):
    """One row per (key, unfit column) of this night, with the raw text exactly as delivered."""
    key = DIMS[dim]["key"]
    parts = [
        raw.where(F.expr(unfit(c, kind))).select(
            F.lit(dim).alias("dimension"),
            F.to_date("dump_date").alias("dump_date"),
            F.col(key).alias("key"),
            F.lit(c).alias("column"),
            F.col(c).alias("raw_value"),
            F.col("_source_file"),
        )
        for c, kind in DIMS[dim]["columns"].items()
    ]
    out = parts[0]
    for part in parts[1:]:
        out = out.unionByName(part)
    return out.withColumn("recorded_at", F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables

# COMMAND ----------


def ensure_properties(table):
    """For a table created before a property was chosen. Checked first: every ALTER is a commit."""
    have = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    for name, value in TABLE_PROPERTIES.items():
        if have.get(name) != value:
            spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('{name}' = '{value}')")
            print(f"{table}: set {name} = {value}")


def create_silver(table, dim):
    spec = DIMS[dim]
    columns = [f"{spec['key']} STRING NOT NULL"]
    columns += [f"{c} {SQL_TYPE[kind]}" for c, kind in spec["columns"].items()]
    columns += [
        "_first_seen_dump_date DATE",  # the night this key first appeared; set once, never updated
        "_last_changed_dump_date DATE",  # the night of the last insert or real change
        "_source_file STRING",  # the Bronze file of that last change
        "_merged_at TIMESTAMP",
    ]
    props = ", ".join(f"'{k}' = '{v}'" for k, v in TABLE_PROPERTIES.items())
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(columns)}) TBLPROPERTIES ({props})")
    ensure_properties(table)


def create_merge_log(log=MERGE_LOG):
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {log} (
            dimension STRING, dump_date DATE, bronze_modified_at TIMESTAMP, source_rows BIGINT,
            inserted BIGINT, updated BIGINT, deleted BIGINT, rows_after BIGINT,
            silver_version BIGINT, padded_zips BIGINT, applied_at TIMESTAMP, held BIGINT)"""
    )
    # Added in Session 9; lines written before it read NULL ("not counted"), not 0.
    if "held" not in spark.table(log).columns:
        spark.sql(f"ALTER TABLE {log} ADD COLUMNS (held BIGINT)")
        print(f"{log}: added column held")


def create_rejected(rejected=REJECTED):
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {rejected} (
            dimension STRING, dump_date DATE, key STRING, column STRING, raw_value STRING,
            _source_file STRING, recorded_at TIMESTAMP)"""
    )


def record_rejected(dim, night, raw, held, rejected=REJECTED):
    """Replace this (dimension, night)'s held rows with tonight's — written with replaceWhere, the rule
    for a re-writable unit (exp_02), so a corrected re-delivery that holds nothing clears them. Skipped
    when there is nothing to write and nothing to clear: an empty overwrite is still a commit."""
    unit = f"dimension = '{dim}' AND dump_date = DATE'{night}'"
    if not held and not spark.table(rejected).where(unit).limit(1).count():
        return
    (
        rejected_rows(dim, raw).write.format("delta").mode("overwrite")
        .option("replaceWhere", unit).saveAsTable(rejected)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## The MERGE, and the guard in front of it

# COMMAND ----------


def merge_sql(target, source, dim, *, delete_missing=True, only_changed=True):
    """The whole statement. The two switches exist for exp_03, which runs the wrong versions on
    purpose; production never passes them."""
    spec = DIMS[dim]
    key, cols = spec["key"], list(spec["columns"])
    # <=> is "equal, counting two NULLs as equal". Plain = gives NULL for NULL = NULL, and
    # NOT (NULL) is NULL, so a row with any NULL would never count as changed.
    same = " AND ".join(f"t.{c} <=> s.{c}" for c in cols)
    # s._ok: a held row matches (so it is not deleted) but may not change anything.
    matched = f"WHEN MATCHED AND s._ok AND NOT ({same})" if only_changed else "WHEN MATCHED AND s._ok"
    sets = [f"t.{c} = s.{c}" for c in cols] + [
        "t._last_changed_dump_date = s._dump_date",
        "t._source_file = s._source_file",
        "t._merged_at = current_timestamp()",
    ]
    insert_cols = [key, *cols, *LINEAGE]
    insert_vals = [f"s.{c}" for c in (key, *cols)] + [
        "s._dump_date", "s._dump_date", "s._source_file", "current_timestamp()",
    ]
    sql = (
        f"MERGE INTO {target} AS t\n"
        f"USING {source} AS s\n"
        f"ON t.{key} = s.{key}\n"
        f"{matched} THEN UPDATE SET {', '.join(sets)}\n"
        f"WHEN NOT MATCHED AND s._ok THEN INSERT ({', '.join(insert_cols)}) VALUES ({', '.join(insert_vals)})"
    )
    if delete_missing:
        sql += "\nWHEN NOT MATCHED BY SOURCE THEN DELETE"
    return sql


def latest_version(table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first().version


def planned_deletes(table, typed, key):
    """Rows Silver holds that tonight's file does not: exactly what NOT MATCHED BY SOURCE deletes."""
    return spark.table(table).join(typed.select(key), key, "left_anti").count()


def check_deletes(table, typed, dim, *, allowed=False):
    """The circuit breaker. Returns (rows now, rows tonight would delete); raises over the limit."""
    current = spark.table(table).count()
    doomed = planned_deletes(table, typed, DIMS[dim]["key"])
    if current and doomed / current > MAX_DELETE_FRACTION and not allowed:
        raise MassDeleteRefused(
            f"{table}: tonight's file would delete {doomed:,} of {current:,} rows "
            f"({doomed / current:.1%}), over the {MAX_DELETE_FRACTION:.0%} limit. Nothing was merged. "
            "A truncated file looks exactly like this. If the deletions are real, allow this night."
        )
    return current, doomed


def merge_night(table, typed, dim, **switches):
    """Run one MERGE; return what it did, read from the table's own history (not recomputed)."""
    before = latest_version(table)
    typed.createOrReplaceTempView("_silver_night")
    spark.sql(merge_sql(table, "_silver_night", dim, **switches))
    merges = (
        spark.sql(f"DESCRIBE HISTORY {table}")
        .where(f"version > {before} AND operation = 'MERGE'")
        .orderBy("version")
        .collect()
    )
    if not merges:  # recorded, not assumed: does a MERGE that changes nothing still commit?
        return {"inserted": 0, "updated": 0, "deleted": 0, "version": None}
    m = merges[0].operationMetrics
    return {
        "inserted": int(m.get("numTargetRowsInserted", 0)),
        "updated": int(m.get("numTargetRowsUpdated", 0)),
        "deleted": int(m.get("numTargetRowsDeleted", 0)),
        "version": merges[0].version,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which nights to apply — one at a time, oldest first, never backwards

# COMMAND ----------


def plan_nights(dim, bronze=BRONZE, log=MERGE_LOG):
    """(nights to apply in order, nights to report and leave alone).

    Applied: every Bronze night newer than the last one in the merge log, plus that last night
    itself if Bronze now holds a newer copy of it (a corrected re-delivery). A night older than the
    last applied one is never applied — Silver would go back in time — only reported.
    """
    in_bronze = {
        r.dump_date: r.modified
        for r in spark.table(f"{bronze}.{dim}")
        .groupBy("dump_date")
        .agg(F.max("_source_modified_at").alias("modified"))
        .collect()
    }
    logged = {
        r.night: r.modified
        for r in spark.table(log)
        .where(F.col("dimension") == dim)
        .groupBy(F.date_format("dump_date", "yyyy-MM-dd").alias("night"))
        .agg(F.max("bronze_modified_at").alias("modified"))
        .collect()
    }
    last = max(logged) if logged else None
    apply, report = [], []
    for night in sorted(in_bronze):
        seen = logged.get(night)
        if last is None or night > last:
            apply.append(night)
        elif night == last and in_bronze[night] > seen:
            apply.append(night)
        elif night < last and (seen is None or in_bronze[night] > seen):
            report.append(night)
    return apply, report


def refuse_lost_log(dim, table, log=MERGE_LOG):
    """A dimension with no line in the merge log may only be applied into an empty Silver table.

    The merge log is this path's checkpoint: it says which night Silver shows. Without it plan_nights
    starts again from the first night, and each old night is MERGEd over today's table — the delete
    breaker may stop one dimension, the others replay their whole history into the change feed (Drill 2).
    """
    if spark.table(log).where(F.col("dimension") == dim).limit(1).count():
        return
    if spark.table(table).limit(1).count():
        raise MergeLogLost(
            f"{table} holds rows, but {log} has no line for {dim!r}: Silver would replay every night from "
            "the first one over today's table. Restore the merge log (time travel), or rebuild this "
            "dimension into an empty table."
        )


def apply_dim(
    dim,
    *,
    allow_mass_delete=(),
    bronze=BRONZE,
    silver=SILVER,
    log=MERGE_LOG,
    rejected=REJECTED,
    guard_reset=True,
):
    """Bring silver.<dim> up to Bronze's newest night. Each night: contract, breaker, MERGE, held rows,
    log. Returns the number of rows held across the nights applied.

    `bronze`, `silver`, `log` and `rejected` exist so exp_03 can run this exact function on scratch
    tables. `guard_reset=False` exists ONLY so Drill 2 can show a lost merge log first.
    """
    table = f"{silver}.{dim}"
    create_silver(table, dim)
    create_rejected(rejected)
    if guard_reset:
        refuse_lost_log(dim, table, log)
    held_total = 0
    apply, older = plan_nights(dim, bronze, log)
    for night in older:
        print(f"{dim:9} {night}: changed in Bronze, but older than what Silver shows — NOT applied")
    if not apply:
        print(f"{dim:9} nothing new")
    for night in apply:
        raw = bronze_night(dim, night, bronze)
        stats, problems = check_night(dim, raw)
        if problems:
            raise NightRefused(
                f"{table}: night {night} refused before its MERGE: {problems}. "
                "Silver still shows the previous night."
            )
        typed = typed_night(dim, raw)
        current, doomed = check_deletes(table, typed, dim, allowed=night in allow_mass_delete)
        result = merge_night(table, typed, dim)
        record_rejected(dim, night, raw, stats["held"], rejected)
        held_total += stats["held"]
        rows_after = spark.table(table).count()

        # Built inside Spark: the Bronze file time is never pulled into Python and written back,
        # so the next run compares it with exactly the value it read (plan_nights).
        def n(value, name):
            return F.lit(value).cast("bigint").alias(name)

        raw.agg(F.max("_source_modified_at").alias("bronze_modified_at")).select(
            F.lit(dim).alias("dimension"),
            F.lit(night).cast("date").alias("dump_date"),
            "bronze_modified_at",
            n(stats["rows"], "source_rows"),
            n(result["inserted"], "inserted"),
            n(result["updated"], "updated"),
            n(result["deleted"], "deleted"),
            n(rows_after, "rows_after"),
            n(result["version"], "silver_version"),
            n(stats["padded_zips"], "padded_zips"),
            F.current_timestamp().alias("applied_at"),
            n(stats["held"], "held"),
        ).write.mode("append").saveAsTable(log)
        share = f"{doomed / current:.2%} of {current:,}" if current else "empty table"
        print(
            f"{dim:9} {night}: {stats['rows']:>6,} rows in file -> +{result['inserted']:,} "
            f"~{result['updated']:,} -{result['deleted']:,} ({share}); {stats['held']:,} held; "
            f"{rows_after:,} in Silver; {stats['padded_zips']:,} zips padded; version {result['version']}"
        )
    return held_total

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks shared by production and exp_03

# COMMAND ----------


def business_columns(dim):
    return [DIMS[dim]["key"], *DIMS[dim]["columns"]]


def compare_to_night(table, typed, dim):
    """(rows in Silver but not in the night, rows in the night but not in Silver). (0, 0) = equal.

    Held keys are left out on both sides: Silver keeps their previous values on purpose.
    """
    cols, key = business_columns(dim), DIMS[dim]["key"]
    held = typed.where(~F.col("_ok")).select(key)
    silver = spark.table(table).select(*cols).join(held, key, "left_anti")
    night = typed.where("_ok").select(*cols)
    return silver.exceptAll(night).count(), night.exceptAll(silver).count()


def change_counts(table, start_version=0):
    """{version: {change type: rows}} from the Change Data Feed."""
    counts = {}
    for r in spark.sql(
        f"SELECT _commit_version AS v, _change_type AS t, count(*) AS n "
        f"FROM table_changes('{table}', {start_version}) GROUP BY 1, 2"
    ).collect():
        counts.setdefault(r.v, {})[r.t] = r.n
    return counts