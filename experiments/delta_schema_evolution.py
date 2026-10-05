# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Delta schema evolution — what each kind of schema change does to our tables
# MAGIC
# MAGIC The Schema Registry contract (`BACKWARD_TRANSITIVE`, Session 7) lets a producer **add a field with a
# MAGIC default**, **remove a field**, or **widen a type** (`int` → `long`). Bronze decodes every message with
# MAGIC the newest schema, so each of those reaches a Delta table as a changed batch. Each part below
# MAGIC makes one change on **scratch copies of real Bronze rows** (`workspace.silver.s11_evo_*`, rebuilt
# MAGIC every run) with the **production Bronze writer** (`_kafka_bronze.make_batch_writer`), and shows
# MAGIC the refusal first:
# MAGIC
# MAGIC - **E1** a new field `channel`: append refused, nothing written → `mergeSchema`: a new column,
# MAGIC   old rows NULL.
# MAGIC - **E2** `schema_version` `int` → `long`: what `mergeSchema` does with it, and with a value an
# MAGIC   `int` cannot hold → `delta.enableTypeWidening`.
# MAGIC - **E3** the new field reaching a Silver `MERGE`: `INSERT *` drops it with no error →
# MAGIC   `MERGE WITH SCHEMA EVOLUTION`.
# MAGIC - **E4** rename / drop a column: refused → **column mapping**, metadata only, no file rewritten.
# MAGIC - **E5** the change feed across that rename: does a reader of the feed survive it?
# MAGIC
# MAGIC Every step that must fail is a **batch** write or an `ALTER`, never a stream (a stream dying in a
# MAGIC cell fails the cell — incidents.md, 2026-09-27). Nothing here touches the live topics, the registry
# MAGIC or production tables.

# COMMAND ----------

# MAGIC %run ../databricks/bronze/_kafka_bronze

# COMMAND ----------

import uuid

BRONZE_ORDERS = "workspace.bronze.orders"
S = "workspace.silver.s11_evo"
TARGET = f"{S}_bronze_orders"  # a scratch Bronze `orders`: same schema, Drill 1's 74 rows
QUARANTINE = f"{S}_bronze_orders_quarantine"
APP_ID = f"ledgerline.s11.evo.{uuid.uuid4().hex[:8]}"
RECORD_FIELDS = [c for c in spark.table(BRONZE_ORDERS).columns if c not in META]

spark.sql(f"CREATE OR REPLACE TABLE {TARGET} AS SELECT * FROM {BRONZE_ORDERS} WHERE _batch_id = 8")
spark.sql(f"CREATE OR REPLACE TABLE {QUARANTINE} AS SELECT * FROM {BRONZE_ORDERS}_quarantine WHERE false")


def version(table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first().version


def columns(table):
    return {f.name: f.dataType.simpleString() for f in spark.table(table).schema.fields}


def attempt(fn):
    """None if `fn` ran; the error's first line if it raised."""
    try:
        fn()
    except Exception as e:  # every refusal is the result being measured
        return str(e).strip().splitlines()[0][:300]
    return None


def decoded_batch(n=10, extra=None, widen=None, value=None):
    """What `decode()` would hand the batch writer for `n` real messages under a changed schema: the
    record as a struct (`_record`), Kafka's coordinates, no quarantine reason. `extra` = (name, value)
    adds a field; `widen` = (name, type) changes one field's type, and `value` replaces its value."""
    fields = []
    for c in RECORD_FIELDS:
        col = F.col(c)
        if widen and c == widen[0]:
            col = (F.lit(value) if value is not None else col).cast(widen[1]).alias(c)
        fields.append(col)
    if extra:
        fields.append(F.lit(extra[1]).alias(extra[0]))
    meta = [c for c in META if c not in ("_batch_id", "_ingested_at")]  # the writer stamps these
    return (
        spark.table(BRONZE_ORDERS).where("_batch_id = 0").orderBy("_kafka_offset").limit(n)
        .select(F.struct(*fields).alias("_record"), *meta,
                F.lit(None).cast("string").alias("_quarantine_reason"))
    )


print(f"{TARGET}: v{version(TARGET)}, {spark.table(TARGET).count()} rows, {len(columns(TARGET))} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## E1 — the producer adds a field
# MAGIC
# MAGIC Ten real messages arrive decoded with a new field `channel = 'web'`. First the writer as Bronze ran
# MAGIC it until today (`merge_schema=False`), then as it runs now.

# COMMAND ----------

batch = decoded_batch(extra=("channel", "web"))
before = version(TARGET)
refused = attempt(lambda: make_batch_writer(TARGET, QUARANTINE, APP_ID, merge_schema=False)(batch, 100))
after_refusal = version(TARGET)
print(f"without mergeSchema: {refused}")
print(f"version {before} -> {after_refusal}")
assert refused is not None and after_refusal == before, "the old writer should refuse and write nothing"

make_batch_writer(TARGET, QUARANTINE, APP_ID)(batch, 100)  # production: merge_schema=True
e1 = spark.table(TARGET).agg(
    F.count(F.lit(1)).alias("rows"),
    F.count_if(F.col("channel").isNull()).alias("channel_null"),
    F.count_if(F.col("channel") == "web").alias("channel_web"),
).first()
print(f"with mergeSchema: version {version(TARGET)}, {e1.asDict()}, "
      f"channel type {columns(TARGET)['channel']}")
assert (e1.rows, e1.channel_null, e1.channel_web) == (84, 74, 10)

# The same batch id again: txnVersion still makes the replay a no-op, schema change or not.
make_batch_writer(TARGET, QUARANTINE, APP_ID)(batch, 100)
assert spark.table(TARGET).count() == 84, "a replayed batch must not land twice"

# COMMAND ----------

# MAGIC %md
# MAGIC ## E2 — the producer widens a type: `schema_version` `int` → `long`
# MAGIC
# MAGIC Avro allows reading an `int` as a `long`, so the registry accepts this change. Delta's `mergeSchema`
# MAGIC adds columns; what it does to a column whose type grew is measured here (first run, S11: no error,
# MAGIC the column stayed `int` — so the question became what happens to a value an `int` cannot hold).

# COMMAND ----------

INT_MAX = 2_147_483_647
wide = decoded_batch(n=5, extra=("channel", "app"), widen=("schema_version", "bigint"))
before = version(TARGET)
small = attempt(lambda: make_batch_writer(TARGET, QUARANTINE, APP_ID)(wide, 101))
print(f"long values that fit an int: {small or 'written'}; version {before} -> {version(TARGET)}; "
      f"schema_version is {columns(TARGET)['schema_version']}")

big = decoded_batch(n=1, extra=("channel", "big"), widen=("schema_version", "bigint"), value=3_000_000_000)
before = version(TARGET)
overflow = attempt(lambda: make_batch_writer(TARGET, QUARANTINE, APP_ID)(big, 102))
landed = spark.table(TARGET).where("channel = 'big'").select("schema_version").collect()
print(f"a long that does not fit (3,000,000,000): {overflow or 'written'}; version {before} -> "
      f"{version(TARGET)}; landed as {[r.schema_version for r in landed]}")

# Type widening: a Delta table feature (it upgrades the table's protocol). Not enabled on production
# Bronze (decisions.md, Session 11) — shown here only to know what it takes.
widening = attempt(lambda: spark.sql(
    f"ALTER TABLE {TARGET} SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')"))
print(f"enable type widening: {widening or 'ok'}")
if widening is None:
    retried = attempt(lambda: make_batch_writer(TARGET, QUARANTINE, APP_ID)(big, 103))
    landed = spark.table(TARGET).where("channel = 'big'").select("schema_version").collect()
    print(f"retry with widening: {retried or 'written'}; schema_version is now "
          f"{columns(TARGET)['schema_version']}; big rows landed as {[r.schema_version for r in landed]}; "
          f"INT_MAX {INT_MAX:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## E3 — the new column reaches a Silver `MERGE`
# MAGIC
# MAGIC A Silver-like table made before `channel` existed. `INSERT *` means "every *target* column from the
# MAGIC source column of the same name" — so a source column the target lacks has nowhere to go.

# COMMAND ----------

SILVER_LIKE = f"{S}_silver_orders"
spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER_LIKE} AS
    SELECT event_id, event_type, order_id, event_ts FROM {BRONZE_ORDERS} WHERE false
""")
src = f"(SELECT event_id, event_type, order_id, event_ts, channel FROM {TARGET} WHERE channel IS NOT NULL)"

plain = attempt(lambda: spark.sql(f"""
    MERGE INTO {SILVER_LIKE} t USING {src} s ON t.event_id = s.event_id
    WHEN NOT MATCHED THEN INSERT *
"""))
print(f"plain MERGE ... INSERT *: {plain or 'no error'}; rows {spark.table(SILVER_LIKE).count()}; "
      f"columns {sorted(columns(SILVER_LIKE))}")

evolving = attempt(lambda: spark.sql(f"""
    MERGE WITH SCHEMA EVOLUTION INTO {SILVER_LIKE} t USING {src} s ON t.event_id = s.event_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
"""))
e3 = spark.table(SILVER_LIKE)
print(f"MERGE WITH SCHEMA EVOLUTION: {evolving or 'no error'}; columns {sorted(columns(SILVER_LIKE))}")
if "channel" in e3.columns:
    print(f"rows with channel: {e3.where('channel IS NOT NULL').count()} of {e3.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## E4 — rename and drop a column: column mapping
# MAGIC
# MAGIC Delta's Parquet files store columns by name, so renaming a column would mean rewriting every file.
# MAGIC **Column mapping** (`delta.columnMapping.mode = 'name'`) gives each column a permanent physical id
# MAGIC in the files and keeps the readable name only in the table's log — after it, a rename or a drop is a
# MAGIC change to the log alone. It upgrades the table's protocol: any reader must understand column mapping.

# COMMAND ----------

MAPPED = f"{S}_mapping"
spark.sql(f"""
    CREATE OR REPLACE TABLE {MAPPED} TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true') AS
    SELECT event_id, event_type, order_id, order_status, event_ts FROM {BRONZE_ORDERS} WHERE _batch_id = 0
""")
spark.sql(f"UPDATE {MAPPED} SET order_status = upper(order_status) WHERE event_type = 'canceled'")


def detail(table):
    d = spark.sql(f"DESCRIBE DETAIL {table}").first()
    return {"files": d.numFiles, "bytes": d.sizeInBytes, "reader": d.minReaderVersion,
            "writer": d.minWriterVersion}


refused = attempt(lambda: spark.sql(f"ALTER TABLE {MAPPED} RENAME COLUMN order_status TO source_status"))
print(f"rename without column mapping: {refused}")
before = detail(MAPPED)
# Only the mode is set: Databricks adds the protocol features it needs (reader and writer versions).
spark.sql(f"ALTER TABLE {MAPPED} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
spark.sql(f"ALTER TABLE {MAPPED} RENAME COLUMN order_status TO source_status")
spark.sql(f"ALTER TABLE {MAPPED} DROP COLUMN event_type")
after = detail(MAPPED)
print(f"before: {before}\nafter:  {after}")
print(f"columns now: {sorted(columns(MAPPED))}")
display(spark.sql(f"DESCRIBE HISTORY {MAPPED}").select("version", "operation", "operationParameters",
                                                         "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## E5 — the change feed across the rename
# MAGIC
# MAGIC Session 13 exports Silver to Snowflake from the change feed. If a Silver column is ever renamed or
# MAGIC dropped, can a batch read of the feed span that change? Versions: 1 = the `UPDATE` (before the
# MAGIC rename), then the mapping, rename and drop, then one more `UPDATE` after them.

# COMMAND ----------

spark.sql(f"UPDATE {MAPPED} SET source_status = lower(source_status) WHERE source_status = 'CANCELED'")
last = version(MAPPED)
across = attempt(lambda: spark.sql(f"SELECT count(*) FROM table_changes('{MAPPED}', 1)").collect())
after_only = attempt(lambda: spark.sql(f"SELECT count(*) FROM table_changes('{MAPPED}', {last})").collect())
print(f"feed from v1 (spans the rename and drop): {across or 'read fine'}")
print(f"feed from v{last} (after them):           {after_only or 'read fine'}")
if after_only is None:
    display(spark.sql(f"SELECT _change_type, count(*) FROM table_changes('{MAPPED}', {last}) GROUP BY 1"))