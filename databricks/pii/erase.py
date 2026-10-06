# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Right to erasure — one request, every table that holds the person
# MAGIC
# MAGIC decisions.md, Session 12 ("Erasure deletes physically …"). For each request, in this order:
# MAGIC
# MAGIC 1. **forget list first** — `pii.erasure_requests`; every PII write reads it in the same statement
# MAGIC    that writes (Bronze's `keep`, the vault MERGE), so nothing brings the person back;
# MAGIC 2. **`DELETE`** in every table with a `pii_subject` column tag — found by asking Unity Catalog,
# MAGIC    not from a list in this notebook;
# MAGIC 3. **`REORG TABLE … APPLY (PURGE)`** — a `DELETE` with deletion vectors only *hides* the row
# MAGIC    inside a file the table still uses (experiments/gdpr_erasure A1); PURGE rewrites that file;
# MAGIC 4. **`VACUUM`** once the purge is older than the tables' 7-day window — before that VACUUM
# MAGIC    would keep the old files anyway. Until then the person exists only in files no current
# MAGIC    version uses (time travel to an older version).
# MAGIC
# MAGIC **What is erased:** contact data — name, email, phone, email hash. The pseudonymous
# MAGIC `customer_unique_id`, the customer's city / state and their orders stay: order records are kept
# MAGIC for accounting, and without the vault they no longer lead to a person.
# MAGIC
# MAGIC **Widgets:** `customer_unique_id` + `request_id` to file a new request (leave empty to only
# MAGIC process what is pending); `mode` = `plan` (default, prints what it would do) or `erase`.
# MAGIC Safe to run daily: each step is logged and never repeated.

# COMMAND ----------

from datetime import UTC, datetime, timedelta

from pyspark.sql import functions as F

CATALOG = "workspace"
REQUESTS = f"{CATALOG}.pii.erasure_requests"
LOG = f"{CATALOG}.pii.erasure_log"
WINDOW = timedelta(days=7)  # = deletedFileRetentionDuration on the PII tables (contact_vault)

# Masks and the row filter on the vault apply to every reader, the owner included (governance
# notebook). A writer outside `pii_readers` would see only SP customers and masked values: the
# MERGE would insert the hidden people again, and an erasure DELETE would remove nothing while its
# own check, reading through the same filter, passed. So the writer must see every row.
_writer = spark.sql("SELECT current_user() AS me, is_account_group_member('pii_readers') AS ok").first()
assert _writer.ok, f"{_writer.me} is not in pii_readers: under the row filter this run would write blind"

dbutils.widgets.text("customer_unique_id", "")
dbutils.widgets.text("request_id", "")
dbutils.widgets.dropdown("mode", "plan", ["plan", "erase"])
person = dbutils.widgets.get("customer_unique_id").strip()
request_id = dbutils.widgets.get("request_id").strip()
mode = dbutils.widgets.get("mode")

spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {LOG} (
        request_id STRING, customer_unique_id STRING, table_name STRING,
        step STRING, rows LONG, at TIMESTAMP
    ) COMMENT 'One line per erasure step per table: deleted, purged, vacuumed, verified.'"""
)


def log(rows):
    if mode == "erase" and rows:
        spark.createDataFrame(
            rows, "request_id STRING, customer_unique_id STRING, table_name STRING, step STRING, rows LONG"
        ).withColumn("at", F.current_timestamp()).write.mode("append").saveAsTable(LOG)


def pii_tables():
    """{table: subject column} for every table Unity Catalog says holds a person's data."""
    return {
        f"{r.catalog_name}.{r.schema_name}.{r.table_name}": r.column_name
        for r in spark.sql(
            f"""SELECT catalog_name, schema_name, table_name, column_name
            FROM {CATALOG}.information_schema.column_tags WHERE tag_name = 'pii_subject'"""
        ).collect()
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. File the request (forget list first)

# COMMAND ----------

if person:
    if not request_id:
        raise ValueError("a new request needs a request_id (the ticket it came from)")
    known = spark.table(REQUESTS).where(F.col("customer_unique_id") == person).count()
    print(f"request {request_id} for {person}: {'already on the forget list' if known else 'new'}")
    if not known and mode == "erase":
        spark.createDataFrame(
            [(request_id, person, datetime.now(UTC), "filed by databricks/pii/erase")],
            "request_id STRING, customer_unique_id STRING NOT NULL, received_at TIMESTAMP, note STRING",
        ).write.mode("append").saveAsTable(REQUESTS)

tables = pii_tables()
print(f"tables tagged pii_subject: {tables}")
assert tables, "no table carries a pii_subject tag — the inventory is empty, so nothing would be erased"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Delete and purge — for every request not yet deleted everywhere

# COMMAND ----------

requests = [(r.request_id, r.customer_unique_id) for r in spark.table(REQUESTS).collect()]
# In plan mode section 1 filed nothing, so the new request is added here in memory — otherwise the
# plan reads an empty forget list and shows nothing about the person it was asked about.
if person and person not in {who for _, who in requests}:
    requests.append((request_id, person))
print(f"requests to process: {len(requests)}")
done = {
    (r.request_id, r.table_name)
    for r in spark.table(LOG).where("step = 'purged'").select("request_id", "table_name").collect()
}
for rid, who in requests:
    for table, column in tables.items():
        if (rid, table) in done:
            continue
        found = spark.table(table).where(F.col(column) == who).count()
        print(f"{rid} {table}: {found} rows" + ("" if mode == "erase" else "  (plan)"))
        if mode != "erase":
            continue
        deleted = 0
        if found:
            spark.sql(f"DELETE FROM {table} WHERE {column} = '{who}'")
            deleted = int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()
                          .operationMetrics.get("numDeletedRows", 0))
            spark.sql(f"REORG TABLE {table} APPLY (PURGE)")
        log([(rid, who, table, "deleted", deleted), (rid, who, table, "purged", deleted)])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Vacuum — once the oldest unvacuumed purge is past the 7-day window

# COMMAND ----------

purged = (
    spark.table(LOG).where("step = 'purged'")
    .groupBy("request_id", "table_name").agg(F.max("at").alias("at"))
)
vacuumed = spark.table(LOG).where("step = 'vacuumed'").select("request_id", "table_name")
waiting = purged.join(vacuumed, ["request_id", "table_name"], "left_anti").collect()
now = datetime.now(UTC).replace(tzinfo=None)
ready = [w for w in waiting if now - w.at >= WINDOW]
for w in waiting:
    print(f"{w.request_id} {w.table_name}: purged {w.at}, "
          + ("VACUUM due" if w in ready else f"VACUUM from {w.at + WINDOW}"))
if mode == "erase":
    for table in sorted({w.table_name for w in ready}):
        spark.sql(f"VACUUM {table}")
        print(f"VACUUM {table}: ran")
    log([(w.request_id, None, w.table_name, "vacuumed", 0) for w in ready])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify — nobody on the forget list is in any tagged table

# COMMAND ----------

erased = spark.table(REQUESTS).select(F.col("customer_unique_id").alias("_who")).distinct()
left = {
    table: spark.table(table).join(erased, F.col(column) == F.col("_who")).count()
    for table, column in tables.items()
}
print(f"people on the forget list: {erased.count()}; still present per table: {left}")
assert not any(left.values()), "an erased person is still in a PII table"
display(spark.table(LOG).orderBy("at", "table_name"))