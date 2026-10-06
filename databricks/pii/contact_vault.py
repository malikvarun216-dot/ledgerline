# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # PII lane — raw contact rows → the vault
# MAGIC
# MAGIC **Synthetic personal data** (name, email, phone — `@example.com`, area code `00`) arrives as a
# MAGIC nightly full snapshot in a lane of its own, `ledgerline/pii/customer_contact/` (decisions.md,
# MAGIC Session 12). Everything personal lives in **one schema, `workspace.pii`**, so access is granted
# MAGIC (or not) in one place:
# MAGIC
# MAGIC - `pii.customer_contact_raw` — every night as delivered (Bronze; the dims' Auto Loader code)
# MAGIC - `pii.customer_contact` — **the vault**: one row per person, newest night (Silver)
# MAGIC - `pii.erasure_requests` — the **forget list**: who asked to be erased. Read *before* every
# MAGIC   write, so no rebuild and no late file brings them back.
# MAGIC
# MAGIC **Pseudonymised analytics:** every other table already identifies a person only by
# MAGIC `customer_unique_id` (Olist's own random id). The mapping from that id to a real person exists
# MAGIC only here. The vault also carries `email_hash` — a hash of the email with a secret salt — so an
# MAGIC analyst can count or join on email without reading it.
# MAGIC
# MAGIC **Differences from the dims, each on purpose:** files kept 7 days for time travel, not 30 (an
# MAGIC erased person must physically leave sooner); **no Change Data Feed** (it would copy every deleted
# MAGIC row into side files); a forget-list filter at Bronze, the one filter Bronze ever applies.
# MAGIC
# MAGIC **Safe to "Run all" at any time:** a night already applied is not applied again.

# COMMAND ----------

# MAGIC %run ../bronze/_autoload_dims

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
PII = f"{CATALOG}.pii"
RAW = f"{PII}.customer_contact_raw"
VAULT = f"{PII}.customer_contact"
REQUESTS = f"{PII}.erasure_requests"
VAULT_LOG = f"{PII}.vault_merge_log"
LANDING = "s3://ledgerline-landing-dev-fffc8b65/ledgerline/pii/customer_contact/"
STATE = f"/Volumes/{CATALOG}/pii/checkpoints/customer_contact"
KEY = "customer_unique_id"
CONTACT_COLUMNS = ["customer_name", "customer_email", "customer_phone"]
# An erased person's rows stay in replaced files until VACUUM removes them, and VACUUM only removes
# files older than this. 7 days (Delta's default) instead of the project's 30: personal data
# leaves sooner, at the price of a shorter time-travel window on these tables only.
PII_RETENTION = "interval 7 days"
MAX_DELETE_FRACTION = 0.05  # same breaker as the dims: a truncated file must not empty the vault

# Where each personal column is, as Unity Catalog tags — the erasure job finds tables by these
# tags, not by a list someone has to remember to update. `pii_subject` marks the column that says
# whose data a row is.
COLUMN_TAGS = {
    "customer_unique_id": {"pii_subject": "customer"},
    "customer_name": {"pii": "name"},
    "customer_email": {"pii": "email"},
    "customer_phone": {"pii": "phone"},
    "email_hash": {"pii": "pseudonym"},
}

# Masks and the row filter on the vault apply to every reader, the owner included (governance
# notebook). A writer outside `pii_readers` would see only SP customers and masked values: the
# MERGE would insert the hidden people again, and an erasure DELETE would remove nothing while its
# own check, reading through the same filter, passed. So the writer must see every row.
_writer = spark.sql("SELECT current_user() AS me, is_account_group_member('pii_readers') AS ok").first()
assert _writer.ok, f"{_writer.me} is not in pii_readers: under the row filter this run would write blind"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {PII} COMMENT 'Personal data (synthetic). Grants here only.'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {PII}.checkpoints")
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {REQUESTS} (
        request_id STRING NOT NULL,
        customer_unique_id STRING NOT NULL,
        received_at TIMESTAMP NOT NULL,
        note STRING
    ) COMMENT 'Forget list: people who asked to be erased. Read before every PII write.'"""
)
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {VAULT} (
        customer_unique_id STRING NOT NULL,
        customer_name STRING,
        customer_email STRING,
        customer_phone STRING,
        email_hash STRING,
        dim_updated_at TIMESTAMP,
        _dump_date STRING,
        _merged_at TIMESTAMP
    ) COMMENT 'The vault: current contact data per person (synthetic). No change feed, on purpose.'
    TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '{PII_RETENTION}')"""
)
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {VAULT_LOG} (
        dump_date STRING, applied_at TIMESTAMP,
        inserted LONG, updated LONG, deleted LONG, rows_after LONG
    )"""
)


def not_erased(batch_df):
    """Drop every row of a person on the forget list. Takes its session from the DataFrame:
    inside foreachBatch on serverless the notebook's `spark` is the wrong one (incidents.md
    2026-09-26)."""
    erased = batch_df.sparkSession.table(REQUESTS).select(KEY)
    return batch_df.join(erased, KEY, "left_anti")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze — every delivered night, minus the forget list

# COMMAND ----------

ingest(LANDING, RAW, STATE, allow_overwrites=True, keep=not_erased, retention=PII_RETENTION)
display(spark.table(RAW).groupBy("dump_date").count().orderBy("dump_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The vault — the newest night, one row per person
# MAGIC
# MAGIC Full snapshot, so the dims' three-clause MERGE: insert the new, update the changed, **delete the
# MAGIC absent** (a customer the source deleted). Only the newest night matters — the vault keeps no
# MAGIC history of anyone's contact data, on purpose.

# COMMAND ----------


def contact_tonight(newest):
    salt = dbutils.secrets.get(catalog=CATALOG, schema="ledgerline_secrets", key="pii_hash_salt")
    rows = (
        spark.table(RAW)
        .where(F.col("dump_date") == newest)
        .join(spark.table(REQUESTS).select(KEY), KEY, "left_anti")
    )
    bad_keys = rows.where(F.col(KEY).isNull()).count()
    dupes = rows.groupBy(KEY).count().where("count > 1").count()
    if bad_keys or dupes or rows.isEmpty():
        raise ValueError(f"night {newest} refused: {bad_keys} NULL keys, {dupes} duplicated keys")
    return rows.select(
        KEY,
        *CONTACT_COLUMNS,
        # Lower-cased and trimmed first, so "Ana@X.com " and "ana@x.com" hash the same.
        F.sha2(F.concat(F.lit(salt), F.lower(F.trim("customer_email"))), 256).alias("email_hash"),
        F.to_timestamp("dim_updated_at").alias("dim_updated_at"),
        F.col("dump_date").alias("_dump_date"),
    )


def merge_vault():
    newest = spark.table(RAW).agg(F.max("dump_date")).first()[0]
    applied = {r.dump_date for r in spark.table(VAULT_LOG).select("dump_date").collect()}
    if newest is None or newest in applied:
        print(f"vault: newest night {newest} already applied — nothing to do")
        return
    source = contact_tonight(newest)
    source.createOrReplaceTempView("contact_tonight")

    vault_rows = spark.table(VAULT).count()
    planned_deletes = spark.table(VAULT).join(source.select(KEY), KEY, "left_anti").count()
    if vault_rows and planned_deletes / vault_rows > MAX_DELETE_FRACTION:
        raise ValueError(f"vault: night {newest} would delete {planned_deletes} of {vault_rows} rows")

    compared = [*CONTACT_COLUMNS, "email_hash", "dim_updated_at"]
    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in compared)
    columns = [KEY, *compared, "_dump_date"]
    # The forget list is read again INSIDE the MERGE statement, not only when the source was built:
    # experiments/gdpr_erasure D1/D2 — a MERGE whose source was fixed before an erasure put the
    # person back with no error, and a retried MERGE did the same. Read here, an erased person is
    # never matched, so the MERGE also never conflicts with the erasure's DELETE (D2: both commit).
    not_forgotten = f"(SELECT * FROM contact_tonight WHERE {KEY} NOT IN (SELECT {KEY} FROM {REQUESTS}))"
    metrics = spark.sql(
        f"""MERGE INTO {VAULT} t USING {not_forgotten} s ON t.{KEY} = s.{KEY}
        WHEN MATCHED AND ({changed}) THEN UPDATE SET
            {", ".join(f"t.{c} = s.{c}" for c in columns[1:])}, t._merged_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT ({", ".join(columns)}, _merged_at)
            VALUES ({", ".join(f"s.{c}" for c in columns)}, current_timestamp())
        WHEN NOT MATCHED BY SOURCE THEN DELETE"""
    ).first()
    rows_after = spark.table(VAULT).count()
    spark.createDataFrame(
        [(newest, metrics.num_inserted_rows, metrics.num_updated_rows, metrics.num_deleted_rows, rows_after)],
        "dump_date STRING, inserted LONG, updated LONG, deleted LONG, rows_after LONG",
    ).withColumn("applied_at", F.current_timestamp()).select(
        "dump_date", "applied_at", "inserted", "updated", "deleted", "rows_after"
    ).write.mode("append").saveAsTable(VAULT_LOG)
    print(f"vault: night {newest} inserted {metrics.num_inserted_rows}, updated {metrics.num_updated_rows}, "
          f"deleted {metrics.num_deleted_rows}, rows now {rows_after}")


merge_vault()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tags — where personal data is, recorded in Unity Catalog
# MAGIC
# MAGIC Set only when missing. `information_schema.column_tags` then answers "which columns of which
# MAGIC tables hold personal data?" with one query — the erasure job's inventory.

# COMMAND ----------


def ensure_tags(table):
    catalog, schema, name = table.split(".")
    have = {
        (r.column_name, r.tag_name, r.tag_value)
        for r in spark.sql(
            f"""SELECT column_name, tag_name, tag_value FROM {catalog}.information_schema.column_tags
            WHERE schema_name = '{schema}' AND table_name = '{name}'"""
        ).collect()
    }
    columns = set(spark.table(table).columns)
    for column, tags in COLUMN_TAGS.items():
        missing = {k: v for k, v in tags.items() if (column, k, v) not in have}
        if column in columns and missing:
            pairs = ", ".join(f"'{k}' = '{v}'" for k, v in missing.items())
            spark.sql(f"ALTER TABLE {table} ALTER COLUMN {column} SET TAGS ({pairs})")
            print(f"{table}.{column}: tagged {missing}")
    table_tags = spark.sql(
        f"""SELECT tag_name FROM {catalog}.information_schema.table_tags
        WHERE schema_name = '{schema}' AND table_name = '{name}' AND tag_name = 'contains_pii'"""
    )
    if table_tags.isEmpty():
        # Only when missing: the job identity (ledgerline-jobs) then needs no APPLY TAG on a normal run.
        spark.sql(f"ALTER TABLE {table} SET TAGS ('contains_pii' = 'true')")


for table in (RAW, VAULT):
    ensure_tags(table)

display(
    spark.sql(
        f"""SELECT table_name, column_name, tag_name, tag_value
        FROM {CATALOG}.information_schema.column_tags
        WHERE schema_name = 'pii' ORDER BY table_name, column_name"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify — counts, the forget list, and what the vault looks like

# COMMAND ----------

per_night = spark.table(RAW).groupBy("dump_date").agg(F.count("*").alias("n"))
nights = {r.dump_date: r.n for r in per_night.collect()}
erased = spark.table(REQUESTS).select(KEY).distinct()
vault = spark.table(VAULT)
newest = max(nights)
expected_vault = spark.table(RAW).where(F.col("dump_date") == newest).count()

checks = {
    "raw nights": dict(sorted(nights.items())),
    "vault rows": vault.count(),
    "vault = newest night": vault.count() == expected_vault,
    "distinct email_hash = rows": vault.select("email_hash").distinct().count() == vault.count(),
    "people on the forget list": erased.count(),
    "forget-listed people still in raw": spark.table(RAW).join(erased, KEY).count(),
    "forget-listed people still in vault": vault.join(erased, KEY).count(),
}
for name, value in checks.items():
    print(f"{name:38} {value}")
assert checks["vault = newest night"] and checks["distinct email_hash = rows"]
# The race this guards: an erasure DELETE commits while a vault MERGE that read the forget list
# before it is still running, and the MERGE inserts the person back (experiments/gdpr_erasure).
assert checks["forget-listed people still in raw"] == 0, "an erased person is back in raw"
assert checks["forget-listed people still in vault"] == 0, "an erased person is back in the vault"

history = spark.sql(f"DESCRIBE HISTORY {VAULT}")
display(history.select("version", "timestamp", "operation", "operationMetrics"))