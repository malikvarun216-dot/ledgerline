# Databricks notebook source
# MAGIC %md
# MAGIC # Drill 1 — dimension dumps and D4, part 1 of 2
# MAGIC
# MAGIC **Run after:** the 4th night has landed (`dim_dumps.py --sink s3 --nights 4`), production
# MAGIC `autoload_dims` has run once, and `land_drill1_scratch.py reviews` + `redelivery-original` are done.
# MAGIC
# MAGIC 1. **New data only (live).** The 4th night (2017-08-30) is a new file per dimension. The
# MAGIC    generator also rewrote nights 1 to 3 in place, byte-identical (MD5 = ETag, checked before the
# MAGIC    write). Expected: one new WRITE per table, for 2017-08-30 only; the rewritten files are not
# MAGIC    re-read, because Auto Loader tracks files by path.
# MAGIC 2. **Checkpoint lost (live).** Delete every dims checkpoint and re-run: Auto Loader forgets and
# MAGIC    re-reads all 12 files. `replaceWhere` must make that harmless — counts unchanged. This is the
# MAGIC    Session 6 proof, moved out of the production notebook. It runs on live tables because the
# MAGIC    guarantee claims the attack is harmless; the Kafka checkpoint reset ran on scratch because
# MAGIC    its known outcome is data loss.
# MAGIC 3. **Re-delivery, first load (scratch).** Two real seller nights copied to `ledgerline/drill1/`.
# MAGIC 4. **D4 (scratch).** The reviews CSV — 99,224 records, 3,852 of them with line breaks inside
# MAGIC    quoted text — through Auto Loader three ways.

# COMMAND ----------

# MAGIC %run ../databricks/bronze/_autoload_dims

# COMMAND ----------

import json

CATALOG = "workspace"
SCHEMA = "bronze"
BUCKET = "s3://ledgerline-landing-dev-fffc8b65/ledgerline"
LANDING = f"{BUCKET}/dims"
STATE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/dims"
DIMS = ["customer", "product", "seller"]
NEW_NIGHT = "2017-08-30"
OLD_NIGHTS = ["2016-09-04", "2017-01-02", "2017-05-02"]
ORIGINAL_LANDING_DAY = "2026-09-20"
SCRATCH = f"{BUCKET}/drill1"
SCRATCH_STATE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/drill1"


def rm_if_present(path):
    try:
        dbutils.fs.rm(path, True)
    except Exception as e:
        text = f"{type(e).__name__} {e}".lower()
        if not any(s in text for s in ("not found", "filenotfound", "no such file")):
            raise


def files_seen(state):
    """What Auto Loader's checkpoint says it has ingested: path -> commit time. None if unreadable."""
    try:
        rows = spark.sql(f"SELECT * FROM cloud_files_state('{state}/checkpoint')").collect()
    except Exception as e:  # recorded, not fatal: cloud_files_state on Free Edition is untested
        print(f"cloud_files_state unavailable: {type(e).__name__}: {str(e)[:200]}")
        return None
    return {r.path: r.commit_time for r in rows}


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. New data only — the 4th night, after production `autoload_dims` ran

# COMMAND ----------

for dim in DIMS:
    table = f"{CATALOG}.{SCHEMA}.{dim}"
    writes = replace_commits(table)
    print(f"\n{dim}: WRITE commits (version, predicate, rows):")
    for w in writes:
        print(f"   {w}")
    last_version, last_predicate, last_rows = writes[-1]
    counts = night_counts(table)
    assert NEW_NIGHT in last_predicate and not any(d in last_predicate for d in OLD_NIGHTS), (
        f"{dim}: the newest write should replace {NEW_NIGHT} only"
    )
    assert int(last_rows) == counts[NEW_NIGHT]
    # `_source_modified_at` is the file's modification time when Auto Loader read it. The old files
    # were first landed on 2026-09-20 (S3 LastModified 07:37:51-59) and rewritten today, seconds
    # BEFORE the new night — so "older than the new night" would not prove anything. Rows still
    # carrying the original landing day prove the rewritten files were not read again.
    old = (
        spark.table(table)
        .where(F.col("dump_date").isin(OLD_NIGHTS))
        .agg(F.max("_source_modified_at").alias("newest"), F.count("*").alias("n"))
        .collect()[0]
    )
    new = (
        spark.table(table)
        .where(F.col("dump_date") == NEW_NIGHT)
        .agg(F.min("_source_modified_at"))
        .first()[0]
    )
    print(f"   old nights: {old.n:,} rows, newest source modification time {old.newest}")
    print(f"   new night:  {counts[NEW_NIGHT]:,} rows, source modification time {new}")
    assert str(old.newest.date()) == ORIGINAL_LANDING_DAY, f"{dim}: an old night was re-read"
    seen = files_seen(f"{STATE}/{dim}")
    if seen is not None:
        print(f"   Auto Loader has ingested {len(seen)} file(s): {sorted(p.split('/')[-2] for p in seen)}")
print("\nNEW DATA ONLY: one write per table, for the new night; the three rewritten files were skipped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Checkpoint lost — delete every dims checkpoint, re-run, counts must not move

# COMMAND ----------

before = {dim: night_counts(f"{CATALOG}.{SCHEMA}.{dim}") for dim in DIMS}
writes_before = {dim: len(replace_commits(f"{CATALOG}.{SCHEMA}.{dim}")) for dim in DIMS}
print(f"before: {json.dumps(before, sort_keys=True)}")

rm_if_present(STATE)
for dim in DIMS:
    ingest(f"{LANDING}/{dim}/", f"{CATALOG}.{SCHEMA}.{dim}", f"{STATE}/{dim}")

for dim in DIMS:
    table = f"{CATALOG}.{SCHEMA}.{dim}"
    after = night_counts(table)
    new_writes = replace_commits(table)[writes_before[dim] :]
    replaced = " ".join(p for _, p, _ in new_writes)
    print(f"{dim}: counts {'UNCHANGED' if after == before[dim] else 'CHANGED'}; new writes {new_writes}")
    assert after == before[dim], f"{dim}: a forced re-read changed the counts"
    assert all(d in replaced for d in [*OLD_NIGHTS, NEW_NIGHT]), f"{dim}: expected all 4 nights replaced"
print("CHECKPOINT LOST, HARMLESS: every file re-read, every night replaced by itself, no count moved.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Re-delivery — first load of two real seller nights (scratch)

# COMMAND ----------

REDELIVERY_TABLE = f"{CATALOG}.{SCHEMA}.drill1_seller"
REDELIVERY_SOURCE = f"{SCRATCH}/redelivery/seller/"
REDELIVERY_STATE = f"{SCRATCH_STATE}/redelivery"

spark.sql(f"DROP TABLE IF EXISTS {REDELIVERY_TABLE}")
rm_if_present(REDELIVERY_STATE)
ingest(REDELIVERY_SOURCE, REDELIVERY_TABLE, REDELIVERY_STATE)
counts = night_counts(REDELIVERY_TABLE)
print(counts)
assert counts == {"2017-01-02": 3_095, "2017-05-02": 3_095}
display(
    spark.table(REDELIVERY_TABLE)
    .where("dump_date = '2017-05-02'")
    .orderBy("seller_id")
    .select("seller_id", "seller_city", "dump_date", "_source_modified_at")
    .limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. D4 — the reviews CSV through Auto Loader, three ways
# MAGIC
# MAGIC Graded against pandas on the laptop (a different parser): **99,224** records, every
# MAGIC `review_id` a 32-character hex id, **3,852** messages keeping a line break, **172** messages
# MAGIC containing a `"`, no empty `review_answer_timestamp`.

# COMMAND ----------

REVIEWS = f"{SCRATCH}/reviews/"
VARIANTS = {
    "default": {},
    "multiline": {"multiLine": "true"},
    "multiline_escape": {"multiLine": "true", "escape": '"'},
}


def load_reviews(name, options):
    table = f"{CATALOG}.{SCHEMA}.drill1_reviews_{name}"
    state = f"{SCRATCH_STATE}/reviews_{name}"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    rm_if_present(state)
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaLocation", f"{state}/schema")
    )
    for key, value in options.items():
        reader = reader.option(key, value)
    (
        reader.load(REVIEWS)
        .writeStream.option("checkpointLocation", f"{state}/checkpoint")
        .trigger(availableNow=True)
        .toTable(table)
        .awaitTermination()
    )
    return table


grades = {}
for name, options in VARIANTS.items():
    table = load_reviews(name, options)
    r = spark.sql(
        f"""
        SELECT count(*) AS rows,
               count_if(review_id RLIKE '^[0-9a-f]{{32}}$') AS good_ids,
               count_if(contains(review_comment_message, char(10))) AS messages_with_newline,
               count_if(contains(review_comment_message, '"')) AS messages_with_quote,
               count_if(review_answer_timestamp IS NULL) AS no_answer_ts,
               count_if(review_score NOT IN ('1', '2', '3', '4', '5') OR review_score IS NULL) AS bad_scores,
               count_if(_rescued_data IS NOT NULL) AS rescued
        FROM {table}
        """
    ).collect()[0]
    grades[name] = r.asDict()
    print(f"{name:17} {grades[name]}")

EXPECTED_REVIEWS = {
    "rows": 99_224,
    "good_ids": 99_224,
    "messages_with_newline": 3_852,
    "messages_with_quote": 172,
    "no_answer_ts": 0,
    "bad_scores": 0,
}
for name, g in grades.items():
    wrong = {k: (g[k], v) for k, v in EXPECTED_REVIEWS.items() if g[k] != v}
    print(f"{name:17} {'MATCHES pandas' if not wrong else f'WRONG (got, want): {wrong}'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What a default-options row looks like when a record was split

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{SCHEMA}.drill1_reviews_default")
    .where("NOT review_id RLIKE '^[0-9a-f]{32}$' OR review_answer_timestamp IS NULL")
    .limit(10)
)
