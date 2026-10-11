# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Delta `SHALLOW CLONE` and `DEEP CLONE` — what a copy shares with its source (Session 16)
# MAGIC
# MAGIC A **clone** is a new Delta table made from another one's state at one version. Two kinds:
# MAGIC
# MAGIC - **`DEEP CLONE`** copies the data files *and* the metadata. A real, independent copy — costs
# MAGIC   storage and time, survives anything done to the source. Re-run it and it copies only the files
# MAGIC   added since: the **incremental replication** that Databricks' disaster-recovery story is built on.
# MAGIC - **`SHALLOW CLONE`** copies only the metadata. Its log points at the **source's** data files. Instant
# MAGIC   and free — and it depends on the source's files staying where they are.
# MAGIC
# MAGIC Snowflake's **zero-copy clone** looks like a shallow clone (instant, no bytes copied), but its
# MAGIC micro-partitions are shared and counted by Snowflake itself, so dropping the source never breaks the
# MAGIC clone. Case F asks whether Delta's shallow clone has that guarantee. Snowflake's half is
# MAGIC `snowflake/experiments/s16_clone.py`.
# MAGIC
# MAGIC Scratch in `workspace.s16` (the S11 rule: experiments never write to `bronze` / `silver`).
# MAGIC Silver is only read.
# MAGIC
# MAGIC Cases and predictions:
# MAGIC - **A** `DEEP CLONE` of a 3-version source: all files copied; same rows; its own history of
# MAGIC   1 version; constraint and table properties carried.
# MAGIC - **B** `SHALLOW CLONE` of it: 0 files copied; same rows; every file read is in the
# MAGIC   **source's** folder.
# MAGIC - **C** `UPDATE` 100 rows of the shallow clone: source unchanged; new files in the **clone's** folder.
# MAGIC - **D** `DEEP CLONE … VERSION AS OF 0`: the source before its update, a past state as its own table.
# MAGIC - **E** source changed, deep clone re-run (`CREATE OR REPLACE`): copies **only** the new files;
# MAGIC   clone = source; clone history 2 versions.
# MAGIC - **F** (deliberate) source rewritten and `VACUUM`ed under a shallow clone: the clone **breaks** on
# MAGIC   read (missing file). Uncertain on Unity Catalog managed tables — measured, not assumed.

# COMMAND ----------

from pyspark.sql import functions as F

S = "workspace.s16"
SILVER_INV = "workspace.silver.inventory"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S} COMMENT 'Session 16 scratch: Delta clone experiment'")

SRC = f"{S}.clone_src"
DEEP = f"{S}.clone_deep"
SHALLOW = f"{S}.clone_shallow"
PAST = f"{S}.clone_v0"
SRC_F = f"{S}.clone_src_f"
SHALLOW_F = f"{S}.clone_shallow_f"


def first_line(error):
    text = str(error).strip()
    return text.splitlines()[0][:300] if text else type(error).__name__


def history(table):
    return spark.sql(f"DESCRIBE HISTORY {table}").orderBy("version").collect()


def last_clone(table):
    """The newest CLONE commit of a table: its parameters and metrics."""
    rows = [h for h in history(table) if "CLONE" in h.operation]
    return rows[-1]


def location(table):
    return spark.sql(f"DESCRIBE DETAIL {table}").first().location


def file_folders(table):
    """Which table folder each file the table reads lives in: {folder: rows}."""
    return {
        r.folder: r.n
        for r in spark.table(table)
        .select(F.regexp_extract("_metadata.file_path", r"^(.*/tables/[^/]+)", 1).alias("folder"))
        .groupBy("folder").agg(F.count("*").alias("n")).collect()
    }


def same_table(a, b):
    """True when two tables hold exactly the same rows (both directions of EXCEPT ALL empty)."""
    ta, tb = spark.table(a), spark.table(b)
    return ta.exceptAll(tb).isEmpty() and tb.exceptAll(ta).isEmpty()


def props(table):
    return {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}


for t in (SHALLOW, SHALLOW_F, DEEP, PAST, SRC, SRC_F):
    spark.sql(f"DROP TABLE IF EXISTS {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The source: Silver's stock in a table of its own, three versions
# MAGIC v0 = a copy of `silver.inventory` · v1 = the production constraint `stock_not_negative` ·
# MAGIC v2 = 500 SKUs sold one more unit. Change Data Feed on, so case A can see whether a clone carries it.

# COMMAND ----------

spark.sql(
    f"""CREATE TABLE {SRC} TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        AS SELECT sku_key, stock_qty, seq FROM {SILVER_INV}"""
)
spark.sql(f"ALTER TABLE {SRC} ADD CONSTRAINT stock_not_negative CHECK (stock_qty >= 0)")
spark.sql(
    f"""UPDATE {SRC} SET stock_qty = stock_qty - 1, seq = seq + 1
        WHERE sku_key IN (SELECT sku_key FROM {SRC} WHERE stock_qty > 0 ORDER BY sku_key LIMIT 500)"""
)
SRC_ROWS = spark.table(SRC).count()
SRC_V = history(SRC)[-1].version
SRC_FILES = spark.sql(f"DESCRIBE DETAIL {SRC}").first().numFiles
print(f"source {SRC}: {SRC_ROWS:,} rows, v{SRC_V}, {SRC_FILES} current file(s), at {location(SRC)}")
for h in history(SRC):
    print(f"  v{h.version} {h.operation}")
assert SRC_V == 2, f"expected v0..v2, got v{SRC_V} (Delta's own OPTIMIZE? find commits by operation)"

# COMMAND ----------

# MAGIC %md
# MAGIC ## A — `DEEP CLONE`

# COMMAND ----------

spark.sql(f"CREATE TABLE {DEEP} DEEP CLONE {SRC}")
a = last_clone(DEEP)
print(f"A: {a.operation} {dict(a.operationParameters)}")
print(f"   metrics {dict(a.operationMetrics)}")
a_hist = history(DEEP)
a_props = props(DEEP)
print(f"   clone history: {[(h.version, h.operation) for h in a_hist]}")
print(f"   constraint carried: {a_props.get('delta.constraints.stock_not_negative')!r}; "
      f"CDF carried: {a_props.get('delta.enableChangeDataFeed')!r}")
print(f"   folders read: {file_folders(DEEP)}")
assert same_table(DEEP, SRC), "a deep clone must hold the source's rows exactly"
assert int(a.operationMetrics["numCopiedFiles"]) == SRC_FILES, "a deep clone copies every current file"
assert int(a.operationMetrics["sourceNumOfFiles"]) == SRC_FILES
assert a_props.get("delta.constraints.stock_not_negative") == "stock_qty >= 0"
assert set(file_folders(DEEP)) == {location(DEEP)}, "a deep clone reads only its own folder"
try:
    spark.sql(f"SELECT count(*) FROM {DEEP} VERSION AS OF 1").first()
    a_tt = "the clone answered for version 1 — it has the source's history"
except Exception as e:
    a_tt = first_line(e)
print(f"   time travel to the source's v1 on the clone: {a_tt}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B — `SHALLOW CLONE`

# COMMAND ----------

spark.sql(f"CREATE TABLE {SHALLOW} SHALLOW CLONE {SRC}")
b = last_clone(SHALLOW)
print(f"B: {b.operation} metrics {dict(b.operationMetrics)}")
b_folders = file_folders(SHALLOW)
print(f"   clone location {location(SHALLOW)}")
print(f"   folders read: {b_folders}")
assert same_table(SHALLOW, SRC)
assert int(b.operationMetrics["numCopiedFiles"]) == 0, "a shallow clone copies no data file"
assert set(b_folders) == {location(SRC)}, "every row of a shallow clone is read from the source's files"

# COMMAND ----------

# MAGIC %md
# MAGIC ## C — write to the shallow clone

# COMMAND ----------

src_sum_before = spark.table(SRC).agg(F.sum("stock_qty")).first()[0]
spark.sql(
    f"""UPDATE {SHALLOW} SET stock_qty = stock_qty + 1000
        WHERE sku_key IN (SELECT sku_key FROM {SHALLOW} ORDER BY sku_key LIMIT 100)"""
)
c_folders = file_folders(SHALLOW)
src_sum_after = spark.table(SRC).agg(F.sum("stock_qty")).first()[0]
shallow_sum = spark.table(SHALLOW).agg(F.sum("stock_qty")).first()[0]
print(f"C: source sum {src_sum_before:,} -> {src_sum_after:,}; clone sum {shallow_sum:,} "
      f"(+{shallow_sum - src_sum_before:,})")
print(f"   folders read now: {c_folders}")
assert src_sum_after == src_sum_before, "a write to a clone must never reach the source"
assert shallow_sum - src_sum_before == 100_000
assert c_folders.get(location(SHALLOW), 0) >= 100, "the rewritten rows live in the clone's own folder"
assert c_folders.get(location(SRC), 0) > 0, "the untouched rows are still read from the source's files"

# COMMAND ----------

# MAGIC %md
# MAGIC ## D — a past version as its own table

# COMMAND ----------

spark.sql(f"CREATE TABLE {PAST} DEEP CLONE {SRC} VERSION AS OF 0")
d = last_clone(PAST)
v0_sum = spark.sql(f"SELECT sum(stock_qty) FROM {SRC} VERSION AS OF 0").first()[0]
past_sum = spark.table(PAST).agg(F.sum("stock_qty")).first()[0]
print(f"D: {dict(d.operationParameters)}; clone sum {past_sum:,} = source v0 {v0_sum:,}; "
      f"source now {src_sum_after:,} (500 units sold since v0)")
past_rule = props(PAST).get("delta.constraints.stock_not_negative")
print(f"   constraint on the v0 clone: {past_rule!r} (added at v1)")
assert past_sum == v0_sum, "the v0 clone must hold the source as it was at v0"
assert v0_sum - src_sum_after == 500

# COMMAND ----------

# MAGIC %md
# MAGIC ## E — incremental deep clone: re-run after the source moves
# MAGIC The DR pattern: a scheduled `CREATE OR REPLACE TABLE replica DEEP CLONE source` copies only
# MAGIC what is new.

# COMMAND ----------

spark.sql(f"INSERT INTO {SRC} SELECT concat('s16-new-', id), 5, 1 FROM range(1000)")
spark.sql(f"CREATE OR REPLACE TABLE {DEEP} DEEP CLONE {SRC}")
e = last_clone(DEEP)
e_src_files = spark.sql(f"DESCRIBE DETAIL {SRC}").first().numFiles
print(f"E: metrics {dict(e.operationMetrics)}")
print(f"   source files now {e_src_files}; clone history {[(h.version, h.operation) for h in history(DEEP)]}")
assert same_table(DEEP, SRC)
assert int(e.operationMetrics["numCopiedFiles"]) < e_src_files, "a re-run copies only files it does not hold"
assert len(history(DEEP)) == 2, "CREATE OR REPLACE keeps the clone's own history"

# COMMAND ----------

# MAGIC %md
# MAGIC ## F — DELIBERATE: the source's files vacuumed from under a shallow clone
# MAGIC A fresh pair this notebook never reads before the VACUUM. A notebook that has read a file keeps
# MAGIC answering from its cache after VACUUM (Session 12), so reading the clone first would hide the break.
# MAGIC
# MAGIC 1. `src_f` written from Silver, retention set to 0 hours (serverless refuses `RETAIN 0 HOURS`;
# MAGIC    the table property is the only way — Session 12).
# MAGIC 2. `shallow_f` = `SHALLOW CLONE src_f` (log only, no data read).
# MAGIC 3. `src_f` overwritten from Silver again (not from itself): every file it held is now "removed"
# MAGIC    in its log.
# MAGIC 4. `VACUUM src_f`: the removed files are deleted from storage — if Delta lets it.
# MAGIC 5. Read `shallow_f`.

# COMMAND ----------

spark.sql(
    f"""CREATE TABLE {SRC_F}
        TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 0 hours',
                       'delta.logRetentionDuration' = 'interval 1 days')
        AS SELECT sku_key, stock_qty, seq FROM {SILVER_INV}"""
)
spark.sql(f"CREATE TABLE {SHALLOW_F} SHALLOW CLONE {SRC_F}")
spark.sql(f"INSERT OVERWRITE {SRC_F} SELECT sku_key, stock_qty, seq FROM {SILVER_INV}")
vac = spark.sql(f"VACUUM {SRC_F}").collect()
vac_hist = [h for h in history(SRC_F) if h.operation.startswith("VACUUM")]
for h in vac_hist:
    print(f"F: src v{h.version} {h.operation} {dict(h.operationMetrics or {})}")
try:
    f_rows = spark.table(SHALLOW_F).count()
    f_seen = f"READ OK: {f_rows:,} rows — the source's VACUUM kept the files a shallow clone uses"
except Exception as e:
    f_seen = f"BROKE: {first_line(e)}"
print(f"F: read of the shallow clone after the source's VACUUM -> {f_seen}")
print("   Confirm on fresh compute (SQL warehouse), not trusted from this session alone:")
print(f"   SELECT count(*) FROM {SHALLOW_F};")
print(f"   Repair, if broken: CREATE OR REPLACE TABLE {SHALLOW_F} SHALLOW CLONE {SRC_F};")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

am = a.operationMetrics
print(f"A deep:    copied {am['numCopiedFiles']} of {am['sourceNumOfFiles']} files; "
      f"history {len(a_hist)}; time travel to source v1 -> {a_tt}")
print(f"B shallow: copied {b.operationMetrics['numCopiedFiles']}; reads only the source folder")
own = c_folders.get(location(SHALLOW))
print(f"C write:   source unchanged; clone +100,000 units, {own} rows from its own folder")
print(f"D v0:      clone sum {past_sum:,} = source v0")
print(f"E re-run:  copied {e.operationMetrics['numCopiedFiles']} of {e_src_files} files")
print(f"F vacuum:  {f_seen}")