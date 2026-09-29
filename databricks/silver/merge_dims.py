# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver — customer, product, seller as they are now
# MAGIC
# MAGIC **Pattern:** one `MERGE` per night, oldest first, with `WHEN NOT MATCHED BY SOURCE DELETE` —
# MAGIC the snapshot path. The code is in `_merge_dims`; this notebook runs it and checks the result.
# MAGIC
# MAGIC **Safe to "Run all" at any time.** A night already applied is not applied again (the merge log
# MAGIC `silver.dims_merge_log` records each one), so a re-run with no new night writes nothing.
# MAGIC
# MAGIC **If a night is refused** — a bad value, a duplicate key, or more than 5% of the table deleted —
# MAGIC nothing is merged for that dimension and Silver keeps the previous night. For a *real* mass
# MAGIC deletion, add the night to `ALLOW_MASS_DELETE` below, in a commit, so the decision is on record.

# COMMAND ----------

# MAGIC %run ./_merge_dims

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")
create_merge_log()

# A night listed here may delete more than MAX_DELETE_FRACTION. Empty unless a person decided.
ALLOW_MASS_DELETE = {"customer": [], "product": [], "seller": []}


def run_all():
    refused = []
    for dim in DIMS:  # one dimension's bad night does not hold back the others
        try:
            apply_dim(dim, allow_mass_delete=ALLOW_MASS_DELETE[dim])
        except (NightRefused, MassDeleteRefused) as e:
            refused.append(str(e))
    if refused:
        raise RuntimeError("\n".join(refused))


run_all()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify 1 — each night's MERGE did what the generator says happened that night
# MAGIC
# MAGIC The generator compares each night with the one before in **pandas** and prints new / changed /
# MAGIC gone. Those are exactly the inserts, updates and deletes the MERGE should report — counted by a
# MAGIC different engine, from the files, before Spark ever saw them. A landed night never changes, so
# MAGIC its numbers may be pinned. Checked against the **first** time each night was applied.

# COMMAND ----------

# (inserted, updated, deleted) per night, from `python generators/dim_dumps.py ... --nights 5`.
GOLDEN_MERGE = {
    "customer": {
        "2016-09-04": (1, 0, 0), "2017-01-02": (325, 0, 0), "2017-05-02": (7_742, 0, 0),
        "2017-08-30": (14_429, 10, 0), "2017-12-28": (21_212, 33, 19),
    },
    "product": {
        "2016-09-04": (32_951, 0, 0), "2017-01-02": (0, 25, 0), "2017-05-02": (0, 50, 0),
        "2017-08-30": (0, 50, 0), "2017-12-28": (0, 50, 100),
    },
    "seller": {
        "2016-09-04": (3_095, 0, 0), "2017-01-02": (0, 25, 0), "2017-05-02": (0, 50, 0),
        "2017-08-30": (0, 50, 0), "2017-12-28": (0, 50, 100),
    },
}

first_applied = {
    (r.dimension, r.night): (r.inserted, r.updated, r.deleted)
    for r in spark.sql(
        f"""SELECT dimension, date_format(dump_date, 'yyyy-MM-dd') AS night,
                   min_by(inserted, applied_at) AS inserted, min_by(updated, applied_at) AS updated,
                   min_by(deleted, applied_at) AS deleted
            FROM {MERGE_LOG} GROUP BY 1, 2"""
    ).collect()
}
wrong = []
for dim, nights in GOLDEN_MERGE.items():
    for night, expected in nights.items():
        got = first_applied.get((dim, night))
        flag = "OK" if got == expected else "MISMATCH"
        print(f"{dim:9} {night}  +ins ~upd -del  expected {expected}  merged {got}  {flag}")
        if got != expected:
            wrong.append((dim, night))
assert not wrong, f"MERGE disagrees with the generator on {wrong}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify 2 — Silver equals the newest night exactly, and every zip has five digits
# MAGIC
# MAGIC Stronger than counts: every business column of every row, both directions. `(0, 0)` means no
# MAGIC row in Silver that the newest file lacks (a missed delete would show on the left), and no row in
# MAGIC the file that Silver lacks or holds differently (a missed insert or update, on the right).

# COMMAND ----------

for dim in DIMS:
    table = f"{SILVER}.{dim}"
    newest = spark.table(f"{BRONZE}.{dim}").agg(F.max("dump_date")).first()[0]
    extra, missing = compare_to_night(table, typed_night(dim, bronze_night(dim, newest)), dim)
    key = DIMS[dim]["key"]
    keys = spark.table(table).agg(F.count("*").alias("n"), F.count_distinct(key).alias("d")).first()
    zips = [c for c, kind in DIMS[dim]["columns"].items() if kind == "zip"]
    bad_zips = sum(
        spark.table(table).where(f"{c} IS NULL OR NOT {c} RLIKE '^[0-9]{{5}}$'").count() for c in zips
    )
    print(f"{dim:9} newest {newest}: extra={extra} missing={missing}  rows={keys.n:,} "
          f"distinct keys={keys.d:,}  zips not five digits={bad_zips}")
    assert (extra, missing) == (0, 0), f"{dim}: Silver differs from night {newest}"
    assert keys.n == keys.d, f"{dim}: duplicate keys in Silver"
    assert bad_zips == 0, f"{dim}: a zip is not five digits"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify 3 — the Change Data Feed agrees with the MERGE log, version by version
# MAGIC
# MAGIC `table_changes` lists every row each commit changed: `insert`, `delete`, and an update as a pair
# MAGIC (`update_preimage` before, `update_postimage` after). If the MERGE rewrote unchanged rows, the
# MAGIC feed would show thousands of update pairs a night instead of the real 25 to 50.

# COMMAND ----------

for dim in DIMS:
    table = f"{SILVER}.{dim}"
    feed = change_counts(table)
    for r in spark.table(MERGE_LOG).where(F.col("dimension") == dim).orderBy("applied_at").collect():
        if r.silver_version is None:
            continue
        got = feed.get(r.silver_version, {})
        seen = (got.get("insert", 0), got.get("update_postimage", 0), got.get("delete", 0))
        logged = (r.inserted, r.updated, r.deleted)
        print(f"{dim:9} {r.dump_date} v{r.silver_version}: log {logged}  feed {seen}")
        assert seen == logged, f"{dim} v{r.silver_version}: feed disagrees"
        assert got.get("update_preimage", 0) == r.updated, f"{dim} v{r.silver_version}: unpaired update"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plain re-run — no new night, so nothing may be written

# COMMAND ----------

versions = {dim: latest_version(f"{SILVER}.{dim}") for dim in DIMS}
log_rows = spark.table(MERGE_LOG).count()
run_all()
after = {dim: latest_version(f"{SILVER}.{dim}") for dim in DIMS}
log_rows_after = spark.table(MERGE_LOG).count()
print(f"versions before {versions}, after {after}; merge log rows {log_rows} -> {log_rows_after}")
assert versions == after, "a re-run with no new night wrote to Silver"
assert log_rows_after == log_rows, "a re-run with no new night added to the merge log"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look at what happened — one MERGE commit per night

# COMMAND ----------

display(spark.table(MERGE_LOG).orderBy("dimension", "dump_date", "applied_at"))

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {SILVER}.seller").select(
        "version", "timestamp", "operation", "operationParameters", "operationMetrics"
    )
)