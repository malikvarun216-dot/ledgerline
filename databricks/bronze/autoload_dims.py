# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bronze — nightly dimension dumps (customer, product, seller)
# MAGIC
# MAGIC **Pattern:** Auto Loader + `Trigger.AvailableNow` + `replaceWhere` on `dump_date` — the code is in
# MAGIC `_autoload_dims`. Source is read-only (`ledgerline_landing` external location); state lives in a
# MAGIC UC volume.
# MAGIC
# MAGIC **Safe to "Run all" at any time.** Session 6's version also held the forced-replay proof
# MAGIC (delete every checkpoint, re-read every file), so "Run all" on the production notebook deleted
# MAGIC production state. That proof now lives in `drills/drill1_dims_1`.

# COMMAND ----------

# MAGIC %run ./_autoload_dims

# COMMAND ----------

LANDING = "s3://ledgerline-landing-dev-fffc8b65/ledgerline/dims"
CATALOG = "workspace"
SCHEMA = "bronze"
STATE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/dims"
DIMS = ["customer", "product", "seller"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")


def run_all():
    for dim in DIMS:
        # Paths unchanged since Session 6 ({STATE}/{dim}/schema and /checkpoint): moving them
        # would start a new stream that re-reads every file.
        ingest(f"{LANDING}/{dim}/", f"{CATALOG}.{SCHEMA}.{dim}", f"{STATE}/{dim}")


run_all()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — every night in the table equals its file in the landing zone
# MAGIC
# MAGIC Two checks, for two different mistakes:
# MAGIC
# MAGIC 1. **Against the landing zone, per night:** a plain batch read of the same files, grouped by
# MAGIC    `dump_date`, must equal the table exactly — no night doubled, none missing, none extra. The
# MAGIC    landing zone is the source of truth, the way the broker is for Kafka; no count is written
# MAGIC    into this notebook, so a new night is not a false failure.
# MAGIC 2. **Golden nights, counted on a laptop by a different parser** (pandas): a landed file never
# MAGIC    changes, so its count may be pinned. Catches a parsing mistake both Spark reads would share.

# COMMAND ----------

GOLDEN = {
    "customer": {"2016-09-04": 1, "2017-01-02": 326, "2017-05-02": 8_068, "2017-08-30": 22_497},
    "product": {"2016-09-04": 32_951, "2017-01-02": 32_951, "2017-05-02": 32_951, "2017-08-30": 32_951},
    "seller": {"2016-09-04": 3_095, "2017-01-02": 3_095, "2017-05-02": 3_095, "2017-08-30": 3_095},
}


def landing_counts(source):
    # recursiveFileLookup turns off folder-name inference, so dump_date=... does not become a
    # second dump_date column next to the file's own.
    files = spark.read.option("header", "true").option("recursiveFileLookup", "true").csv(source)
    return {r.dump_date: r.n for r in files.groupBy("dump_date").agg(F.count("*").alias("n")).collect()}


def check_counts():
    for dim in DIMS:
        table = f"{CATALOG}.{SCHEMA}.{dim}"
        got, landed = night_counts(table), landing_counts(f"{LANDING}/{dim}/")
        golden = {d: n for d, n in GOLDEN[dim].items() if d in landed}
        ok = got == landed and all(got.get(d) == n for d, n in golden.items())
        print(f"{dim:9} {'OK' if ok else 'MISMATCH'}  table={dict(sorted(got.items()))}")
        if got != landed:
            print(f"{'':9} landing={dict(sorted(landed.items()))}")
        assert got == landed, f"{dim}: table and landing zone disagree"
        assert all(got.get(d) == n for d, n in golden.items()), f"{dim}: a golden night changed"


check_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plain re-run — no new files, so nothing may be written

# COMMAND ----------

before = {dim: len(replace_commits(f"{CATALOG}.{SCHEMA}.{dim}")) for dim in DIMS}
run_all()
check_counts()
after = {dim: len(replace_commits(f"{CATALOG}.{SCHEMA}.{dim}")) for dim in DIMS}
print(f"WRITE commits before {before}, after {after}")
assert before == after, "a re-run with no new files wrote something"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look at what happened — each write is its own commit

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.customer").select(
        "version", "timestamp", "operation", "operationParameters", "operationMetrics"
    )
)