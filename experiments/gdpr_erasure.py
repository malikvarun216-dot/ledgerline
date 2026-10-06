# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # GDPR erasure — what "delete this person" really takes on Delta, two ways
# MAGIC
# MAGIC One real person from the vault (`workspace.pii.customer_contact`, synthetic data), copied into
# MAGIC scratch tables in **`workspace.s12`** — this notebook never writes to `pii`, `bronze` or `silver`.
# MAGIC
# MAGIC - **A. `DELETE` the person.** Gone from the table; *not* from time travel — and with deletion
# MAGIC   vectors their row is still inside a live file. `REORG … APPLY (PURGE)` rewrites that file;
# MAGIC   time travel still has them. Only `VACUUM` removes the bytes, after the retention window.
# MAGIC - **B. The change feed on:** the `DELETE` itself copies the person's email into the feed.
# MAGIC - **C. Crypto-shredding:** encrypt per person, delete the key — unreadable at once, everywhere;
# MAGIC   but now the *key* table has the time-travel problem.
# MAGIC - **D. The erasure racing the daily MERGE:** a MERGE that read its source before the erasure
# MAGIC   puts the person back, with no error.
# MAGIC
# MAGIC Every step prints what happened. **Run all**; nothing here fails a cell on purpose.

# COMMAND ----------

import threading
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import functions as F

S = "workspace.s12"
VAULT = "workspace.pii.customer_contact"
RAW = "workspace.pii.customer_contact_raw"
KEY = "customer_unique_id"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S} COMMENT 'Session 12 scratch: GDPR erasure experiment'")

# The person to erase: the first (by id) who is in BOTH raw nights, so two raw copies exist.
X = (
    spark.table(RAW).where("dump_date = '2017-08-30'").select(KEY)
    .intersect(spark.table(RAW).where("dump_date = '2017-12-28'").select(KEY))
    .orderBy(KEY).first()[0]
)
X_EMAIL = spark.table(VAULT).where(F.col(KEY) == X).first().customer_email
print(f"person X = {X}  ({X_EMAIL})")


def first_line(error):
    return str(error).strip().splitlines()[0][:220]


def latest(table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()


def metrics(table, keys):
    row = latest(table)
    return f"v{row.version} {row.operation} " + ", ".join(
        f"{k}={row.operationMetrics.get(k)}" for k in keys if k in row.operationMetrics
    )


def fresh(name, properties):
    table = f"{S}.{name}"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"CREATE TABLE {table} TBLPROPERTIES ({properties}) AS SELECT * FROM {VAULT}")
    return table


def x_rows(table, version=None):
    """X's email as the table holds it (now, or at an old version) — or the error that stops it."""
    at = f" VERSION AS OF {version}" if version is not None else ""
    try:
        rows = spark.sql(f"SELECT customer_email FROM {table}{at} WHERE {KEY} = '{X}'").collect()
        return [r.customer_email for r in rows]
    except Exception as e:
        return f"ERROR {first_line(e)}"


def files_holding_x(table, version=None):
    at = f" VERSION AS OF {version}" if version is not None else ""
    return {
        r.f
        for r in spark.sql(
            f"SELECT DISTINCT _metadata.file_path AS f FROM {table}{at} WHERE {KEY} = '{X}'"
        ).collect()
    }


def live_files(table):
    return {r.f for r in spark.sql(f"SELECT DISTINCT _metadata.file_path AS f FROM {table}").collect()}


def vacuum_now(table):
    """Try to remove every unreferenced file now. The safety check refuses a window shorter than
    the table's own; serverless may refuse the setting that turns the check off. Each refusal is
    printed — it is a finding, not a failure."""
    try:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        print("  conf retentionDurationCheck=false: accepted")
    except Exception as e:
        print(f"  conf retentionDurationCheck=false: REFUSED {first_line(e)}")
    try:
        dry = spark.sql(f"VACUUM {table} RETAIN 0 HOURS DRY RUN").count()
        spark.sql(f"VACUUM {table} RETAIN 0 HOURS")
        print(f"  VACUUM RETAIN 0 HOURS: ran (dry run listed {dry} files)")
        return
    except Exception as e:
        print(f"  VACUUM RETAIN 0 HOURS: REFUSED {first_line(e)}")
    # Second way: make the table's own window zero, then a plain VACUUM.
    zero = "'delta.deletedFileRetentionDuration' = 'interval 0 hours'"
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ({zero})")
    try:
        spark.sql(f"VACUUM {table}")
        print("  table retention 0 hours + VACUUM: ran")
    except Exception as e:
        print(f"  table retention 0 hours + VACUUM: REFUSED {first_line(e)}")


props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {VAULT}").collect()}
print("vault deletion vectors:", props.get("delta.enableDeletionVectors", "(not set)"),
      "| change feed:", props.get("delta.enableChangeDataFeed", "(not set)"),
      "| retention:", props.get("delta.deletedFileRetentionDuration"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Physical delete — `DELETE`, then `REORG … PURGE`, then `VACUUM`

# COMMAND ----------

A = fresh("phys", "'delta.enableDeletionVectors' = 'true'")
v_before = latest(A).version
x_file = files_holding_x(A)
print(f"A0  copy at v{v_before}; X in table: {x_rows(A)}; X's row lives in {len(x_file)} file")

spark.sql(f"DELETE FROM {A} WHERE {KEY} = '{X}'")
print("A1  " + metrics(A, ["numDeletedRows", "numDeletionVectorsAdded", "numRemovedFiles", "numAddedFiles"]))
print(f"    X now: {x_rows(A)}   X at v{v_before} (time travel): {x_rows(A, v_before)}")
print(f"    the file that held X is still part of the CURRENT table: {x_file <= live_files(A)}")

spark.sql(f"REORG TABLE {A} APPLY (PURGE)")
print("A2  " + metrics(A, ["numRemovedFiles", "numAddedFiles", "numDeletionVectorsRemoved"]))
print(f"    that file still part of the current table: {x_file <= live_files(A)}")
print(f"    X at v{v_before} (time travel): {x_rows(A, v_before)}")

print("A3  VACUUM")
vacuum_now(A)
print(f"    X at v{v_before} (time travel): {x_rows(A, v_before)}")
print(f"    X now: {x_rows(A)}; rows now {spark.table(A).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. The change feed keeps its own copy of what a `DELETE` removed

# COMMAND ----------

B = fresh("cdf", "'delta.enableDeletionVectors' = 'true', 'delta.enableChangeDataFeed' = 'true'")
v_b = latest(B).version
spark.sql(f"DELETE FROM {B} WHERE {KEY} = '{X}'")


def x_in_change_feed():
    try:
        rows = spark.sql(
            f"SELECT _change_type, _commit_version, customer_email FROM table_changes('{B}', {v_b + 1}) "
            f"WHERE {KEY} = '{X}'"
        ).collect()
        return [(r._change_type, r._commit_version, r.customer_email) for r in rows]
    except Exception as e:
        return f"ERROR {first_line(e)}"


print(f"B1  X now: {x_rows(B)}   X in the change feed: {x_in_change_feed()}")
spark.sql(f"REORG TABLE {B} APPLY (PURGE)")
print(f"B2  after PURGE, X in the change feed: {x_in_change_feed()}")
vacuum_now(B)
print(f"B3  after VACUUM, X in the change feed: {x_in_change_feed()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Crypto-shredding — each person's data encrypted with their own key; erase = delete the key
# MAGIC
# MAGIC `aes_encrypt(value, key, 'GCM')` is built into Databricks SQL. The key here is 32 random bytes
# MAGIC per person (from `uuid()`); in production it would come from a key service (AWS KMS).

# COMMAND ----------

KEYS = f"{S}.dek"
ENC = f"{S}.contact_enc"
spark.sql(f"DROP TABLE IF EXISTS {KEYS}")
spark.sql(f"DROP TABLE IF EXISTS {ENC}")
spark.sql(
    f"""CREATE TABLE {KEYS} TBLPROPERTIES ('delta.enableDeletionVectors' = 'true') AS
    SELECT {KEY}, unhex(sha2(uuid(), 256)) AS dek FROM {VAULT}"""
)
spark.sql(
    f"""CREATE TABLE {ENC} AS
    SELECT v.{KEY}, aes_encrypt(v.customer_email, k.dek, 'GCM') AS email_enc
    FROM {VAULT} v JOIN {KEYS} k USING ({KEY})"""
)
v_keys = latest(KEYS).version


def x_decrypted(keys_version=None):
    at = f" VERSION AS OF {keys_version}" if keys_version is not None else ""
    try:
        rows = spark.sql(
            f"""SELECT CAST(aes_decrypt(e.email_enc, k.dek, 'GCM') AS STRING) AS email
            FROM {ENC} e JOIN {KEYS}{at} k USING ({KEY}) WHERE e.{KEY} = '{X}'"""
        ).collect()
        return [r.email for r in rows]
    except Exception as e:
        return f"ERROR {first_line(e)}"


same = spark.sql(
    f"SELECT aes_encrypt('{X_EMAIL}', dek, 'GCM') = aes_encrypt('{X_EMAIL}', dek, 'GCM') AS same "
    f"FROM {KEYS} WHERE {KEY} = '{X}'"
).first().same
print(f"C0  X decrypted: {x_decrypted()}; the same email encrypted twice gives the same bytes: {same}")

spark.sql(f"DELETE FROM {KEYS} WHERE {KEY} = '{X}'")
cipher_left = spark.table(ENC).where(F.col(KEY) == X).count()
print(f"C1  key deleted. X's ciphertext rows still in {ENC}: {cipher_left}; decrypted now: {x_decrypted()}")
print(f"    ...but with the key table at v{v_keys} (time travel): {x_decrypted(v_keys)}")
spark.sql(f"REORG TABLE {KEYS} APPLY (PURGE)")
vacuum_now(KEYS)
print(f"C2  after PURGE + VACUUM of the KEY table: decrypt with keys at v{v_keys}: {x_decrypted(v_keys)}")
size = {t: spark.sql(f"DESCRIBE DETAIL {t}").first().sizeInBytes for t in (KEYS, ENC)}
print(f"    bytes: key table {size[KEYS]:,}  vs  encrypted data {size[ENC]:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. The erasure racing the daily vault MERGE
# MAGIC
# MAGIC **D1 — a MERGE that decided its rows before the erasure.** X is new tonight (not yet in the vault).
# MAGIC The MERGE's source was built — forget list checked — *before* X's request arrived. The erasure
# MAGIC writes the request and deletes X (nothing to delete yet); then the MERGE commits and inserts X.
# MAGIC No conflict: the DELETE removed nothing, so the MERGE's commit touches nothing it changed.

# COMMAND ----------

V = f"{S}.vault_race"
REQ = f"{S}.requests"
SAMPLE = 2_000


def reset_race(with_x):
    spark.sql(f"DROP TABLE IF EXISTS {V}")
    where = "" if with_x else f"WHERE {KEY} <> '{X}'"
    spark.sql(
        f"""CREATE TABLE {V} TBLPROPERTIES ('delta.enableDeletionVectors' = 'true') AS
        SELECT * FROM {VAULT} {where} ORDER BY {KEY} LIMIT {SAMPLE}"""
    )
    if with_x and spark.table(V).where(F.col(KEY) == X).isEmpty():
        spark.sql(f"INSERT INTO {V} SELECT * FROM {VAULT} WHERE {KEY} = '{X}'")
    spark.sql(f"CREATE OR REPLACE TABLE {REQ} ({KEY} STRING, received_at TIMESTAMP)")


def tonight(phone_suffix=""):
    """Tonight's source rows: the sample plus X, X's phone optionally changed. Materialised —
    the way a MERGE's source is fixed once it has been read."""
    rows = spark.sql(
        f"""SELECT * FROM {VAULT} WHERE {KEY} IN (SELECT {KEY} FROM {V}) OR {KEY} = '{X}'"""
    ).withColumn(
        "customer_phone",
        F.when(F.col(KEY) == X, F.concat("customer_phone", F.lit(phone_suffix)))
        .otherwise(F.col("customer_phone")),
    )
    return spark.createDataFrame(rows.collect(), rows.schema)


def merge_sql(view, forget_inside=False):
    using = view
    if forget_inside:
        using = f"(SELECT * FROM {view} WHERE {KEY} NOT IN (SELECT {KEY} FROM {REQ}))"
    return f"""MERGE INTO {V} t USING {using} s ON t.{KEY} = s.{KEY}
    WHEN MATCHED AND NOT (t.customer_phone <=> s.customer_phone) THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *"""


def erase_x():
    spark.sql(f"INSERT INTO {REQ} VALUES ('{X}', current_timestamp())")  # forget list FIRST
    spark.sql(f"DELETE FROM {V} WHERE {KEY} = '{X}'")


reset_race(with_x=False)
stale = tonight()
stale.createOrReplaceTempView("stale_source")
before = spark.table(V).where(F.col(KEY) == X).count()
erase_x()
deleted = latest(V).operationMetrics.get("numDeletedRows", "0")
spark.sql(merge_sql("stale_source"))
after = spark.table(V).where(F.col(KEY) == X).count()
print(f"D1  X in vault before {before}; erasure deleted {deleted}; after the stale MERGE: {after}  "
      f"<- {'ERASED PERSON BACK, no error' if after else 'gone'}")
guard = spark.table(V).join(spark.table(REQ), KEY).count()
print(f"    the production guard (forget-listed people in the vault): {guard}")

# The fix inside the MERGE: read the forget list as part of the MERGE statement itself.
reset_race(with_x=False)
tonight().createOrReplaceTempView("stale_source")
erase_x()
spark.sql(merge_sql("stale_source", forget_inside=True))
print(f"D1' forget list read inside the MERGE: X after = {spark.table(V).where(F.col(KEY) == X).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC **D2 — truly at the same instant**, X already in the vault: the erasure `DELETE` and a MERGE that
# MAGIC updates X's phone start together from two threads. One must lose (same row). Then the loser
# MAGIC is retried, the way a failed job task is retried — once with a plain source, once with the
# MAGIC forget list read inside the MERGE.
# MAGIC
# MAGIC **D3** — the same race, but the MERGE changes other people only: row-level concurrency should
# MAGIC let both commit.

# COMMAND ----------


def race(statement_a, statement_b, attempts=4):
    for attempt in range(1, attempts + 1):
        start = latest(V).version
        barrier = threading.Barrier(2)

        def one(name, sql, barrier=barrier):
            barrier.wait()
            try:
                spark.sql(sql)
                return name, "committed"
            except Exception as e:
                return name, first_line(e)

        with ThreadPoolExecutor(2) as pool:
            futures = [pool.submit(one, *statement_a), pool.submit(one, *statement_b)]
            outcomes = dict(f.result() for f in futures)
        commits = (
            spark.sql(f"DESCRIBE HISTORY {V}")
            .where(f"version > {start} AND operation IN ('MERGE', 'DELETE')")
            .select("version", "readVersion", "operation")
            .orderBy("version")
            .collect()
        )
        failed = [n for n, r in outcomes.items() if r != "committed"]
        overlapped = bool(failed) or len({c.readVersion for c in commits}) == 1
        said = "; ".join(f"{n}: {r}" for n, r in outcomes.items())
        print(f"  attempt {attempt}: overlapped={overlapped}; {said}")
        for c in commits:
            print(f"    v{c.version} {c.operation} read v{c.readVersion}")
        if overlapped:
            return outcomes
        spark.sql(f"RESTORE TABLE {V} TO VERSION AS OF {start}")  # the forget list is left as it was
    raise RuntimeError("never overlapped")


for forget_inside in (False, True):
    reset_race(with_x=True)
    tonight(phone_suffix="-new").createOrReplaceTempView("update_x")
    spark.sql(f"INSERT INTO {REQ} VALUES ('{X}', current_timestamp())")  # request first, as production
    label = "forget list inside MERGE" if forget_inside else "plain source"
    print(f"D2 ({label})")
    outcomes = race(("erase", f"DELETE FROM {V} WHERE {KEY} = '{X}'"),
                    ("merge", merge_sql("update_x", forget_inside)))
    for name, result in outcomes.items():
        if result != "committed":  # the retry
            if name == "erase":
                spark.sql(f"DELETE FROM {V} WHERE {KEY} = '{X}'")
            else:
                spark.sql(merge_sql("update_x", forget_inside))
            print(f"    {name} retried: committed")
    print(f"    X in vault at the end: {spark.table(V).where(F.col(KEY) == X).count()}")

reset_race(with_x=True)
others = tonight().withColumn(
    "customer_phone",
    F.when(F.col(KEY) != X, F.concat("customer_phone", F.lit("-o"))).otherwise(F.col("customer_phone")),
)
spark.createDataFrame(others.collect(), others.schema).createOrReplaceTempView("update_others")
print("D3 (MERGE touches everyone except X)")
race(("erase", f"DELETE FROM {V} WHERE {KEY} = '{X}'"), ("merge", merge_sql("update_others")))
print(f"    X in vault at the end: {spark.table(V).where(F.col(KEY) == X).count()}; "
      f"others updated: {spark.table(V).where(F.col('customer_phone').endswith('-o')).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up — the scratch schema goes as one unit (Session 11's rule)
# MAGIC
# MAGIC Left in place after a run so the tables and their histories can be looked at. To remove:
# MAGIC `DROP SCHEMA workspace.s12 CASCADE`.