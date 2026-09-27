# Databricks notebook source
# MAGIC %md
# MAGIC # Kafka → Bronze — shared code for `stream_orders` and `stream_cdc`
# MAGIC
# MAGIC Loaded with `%run ./_kafka_bronze`. **Defines functions only — running it reads nothing.**
# MAGIC
# MAGIC **Pattern:** Kafka `readStream` → `foreachBatch` → Delta **append** with
# MAGIC `txnAppId` + `txnVersion = batch_id`, `Trigger.AvailableNow`.
# MAGIC
# MAGIC How a re-run stays harmless, in three steps:
# MAGIC
# MAGIC 1. Before a batch runs, Spark writes *which Kafka offsets it covers* into the **checkpoint**
# MAGIC    (`offsets/N`). After the batch finishes, it writes "batch N done" (`commits/N`).
# MAGIC 2. If the job dies between the Delta write and "batch N done", the restart re-runs batch N —
# MAGIC    **same `batch_id`, same offsets, same rows**.
# MAGIC 3. **`txnAppId` / `txnVersion`** make that re-run a no-op: Delta records "app X wrote version N"
# MAGIC    inside the table's own log, and silently skips any later write from app X with a version
# MAGIC    `<= N`. Without it, batch N is appended twice.
# MAGIC
# MAGIC Records that will not decode go to a `*_quarantine` table with their Kafka coordinates — never
# MAGIC dropped, and never allowed to stop the batch.

# COMMAND ----------

import json
import traceback
import uuid
from datetime import UTC, datetime

from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

CATALOG = "workspace"
SCHEMA = "bronze"
SECRETS_SCHEMA = "ledgerline_secrets"
STATE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/kafka"
PROGRESS_TABLE = f"{CATALOG}.{SCHEMA}.stream_progress"

# Addresses, not secrets. The keys that open them are Unity Catalog secrets.
BOOTSTRAP = "lkc-k8xr0m2.ap-south-1.aws.public.confluent.cloud:9092"
SCHEMA_REGISTRY_URL = "https://psrc-1j6ww6.ap-south-1.aws.confluent.cloud"

# Spark names its Kafka consumer groups <prefix><random>. A prefix of our own makes them
# recognisable in Confluent's console, and is the name any consumer-group ACL has to match.
GROUP_ID_PREFIX = "ledgerline-bronze-"

# Cap per micro-batch. AvailableNow still reads everything that exists when the run starts,
# just in several batches: ~8 for orders (394,090), ~4 for inventory.cdc (158,625). Several
# batches give txnVersion something to count, and give the crash experiment a batch to kill.
MAX_OFFSETS_PER_TRIGGER = 50_000

# One id per notebook execution, so the progress table can tell runs apart.
RUN_ID = str(uuid.uuid4())

# Columns every Bronze row carries besides the decoded record. Kafka's own coordinates
# (topic, partition, offset) are the identity of a message: they are what "no duplicates,
# no gaps" is checked against.
META = [
    "_kafka_topic",
    "_kafka_partition",
    "_kafka_offset",
    "_kafka_timestamp",
    "_kafka_key",
    "_kafka_headers",
    "_schema_id",
    "_raw_value",
    "_batch_id",
    "_ingested_at",
]


def _secret(key):
    return dbutils.secrets.get(catalog=CATALOG, schema=SECRETS_SCHEMA, key=key)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and decode

# COMMAND ----------


def read_kafka(topic):
    jaas = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{_secret("kafka_api_key")}" password="{_secret("kafka_api_secret")}";'
    )
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas)
        .option("subscribe", topic)
        # Used ONCE, when the checkpoint is brand new. Every later run resumes from the
        # checkpoint and ignores this line — a restart never goes back to "earliest".
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        # If Kafka's retention deletes records this stream has not read yet, stop with an
        # error instead of silently skipping ahead. Bronze claims to hold every record.
        .option("failOnDataLoss", "true")
        .option("groupIdPrefix", GROUP_ID_PREFIX)
        # Headers are part of the message. Empty today; kept so a provenance header added
        # later lands in Bronze without a code change.
        .option("includeHeaders", "true")
        .load()
    )


def _registry_options():
    return {
        "confluent.schema.registry.basic.auth.credentials.source": "USER_INFO",
        "confluent.schema.registry.basic.auth.user.info": (
            f'{_secret("sr_api_key")}:{_secret("sr_api_secret")}'
        ),
        # A payload that will not decode becomes NULL instead of failing the whole batch.
        # The NULL is then routed to quarantine below.
        "mode": "PERMISSIVE",
    }


def decode(raw, subject):
    """Kafka rows -> one row per message: Kafka coordinates, raw bytes, decoded record, verdict.

    Confluent's wire format is: 1 magic byte (0x00), a 4-byte schema id, then the Avro body.
    `from_avro` with a registry subject reads that id and fetches the writer's schema itself.
    """
    framed = F.expr("value IS NOT NULL AND length(value) > 5 AND substring(value, 1, 1) = X'00'")
    df = raw.select(
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
        F.col("key").cast("string").alias("_kafka_key"),
        F.col("headers").alias("_kafka_headers"),
        # Kept on purpose: decoding is an interpretation, and Kafka will not keep the
        # original forever. With the bytes in Bronze, a wrong decode can be redone later.
        F.col("value").alias("_raw_value"),
        framed.alias("_framed"),
    )
    df = df.withColumn(
        "_schema_id",
        F.when(F.col("_framed"), F.expr("cast(conv(hex(substring(_raw_value, 2, 4)), 16, 10) as int)")),
    )
    df = df.withColumn(
        "_record",
        F.when(
            F.col("_framed"),
            from_avro(
                data=F.col("_raw_value"),
                options=_registry_options(),
                subject=subject,
                schemaRegistryAddress=SCHEMA_REGISTRY_URL,
            ),
        ),
    )
    df = df.withColumn(
        "_quarantine_reason",
        F.when(F.col("_raw_value").isNull(), F.lit("null value (tombstone)"))
        .when(~F.col("_framed"), F.lit("not Confluent wire format"))
        # NOT `_record IS NULL`: in PERMISSIVE mode a payload that fails to decode comes back as
        # a record whose fields are all NULL, not as NULL (the self-test showed a truncated
        # message passing as "decoded OK" — incidents.md, 2026-09-27). `event_id` is a required,
        # non-nullable field in both schemas, so a NULL there means the decode failed.
        .when(F.col("_record.event_id").isNull(), F.lit("avro decode failed"))
        .when(F.col("_kafka_key").isNull(), F.lit("missing key")),
    )
    return df.drop("_framed")


def _stamped(df, batch_id):
    return df.withColumn("_batch_id", F.lit(batch_id).cast("long")).withColumn(
        "_ingested_at", F.current_timestamp()
    )


def _good(df):
    return df.where("_quarantine_reason IS NULL").select("_record.*", *META)


def _bad(df):
    return df.where("_quarantine_reason IS NOT NULL").select("_quarantine_reason", *META)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Write — one micro-batch

# COMMAND ----------


def make_batch_writer(table, quarantine_table, app_id, *, idempotent=True, crash_after_batch=None):
    """Return the foreachBatch function.

    `idempotent=False` exists ONLY so exp_01 can show the bug first. Bronze never passes it.
    `crash_after_batch=N` raises right after batch N is committed to Delta and before Spark can
    record the batch as done — the one window where a restart re-runs a batch that already landed.

    One write path, no catalog lookups: on serverless this function runs in a cloned session,
    where `tableExists()` once answered False for a table that existed (incidents.md, 2026-09-26).
    The tables are created before the stream starts.
    """

    def write_batch(batch_df, batch_id):
        stamped = _stamped(batch_df, batch_id)
        # Each target table keeps its own record of (app_id -> last version), so the two
        # writes are protected independently: a crash between them re-runs the batch, the
        # first write is skipped and the second goes through.
        for rows, target in ((_good(stamped), table), (_bad(stamped), quarantine_table)):
            writer = rows.write.format("delta").mode("append")
            if idempotent:
                writer = writer.option("txnAppId", app_id).option("txnVersion", batch_id)
            writer.saveAsTable(target)
        if crash_after_batch is not None and batch_id == crash_after_batch:
            raise RuntimeError(
                f"SIMULATED CRASH after batch {batch_id} was committed to {table}, "
                "before Spark could mark the batch done in the checkpoint"
            )

    return write_batch


def create_tables(decoded, table, quarantine_table):
    """Create empty tables in the notebook's own session, before any batch runs."""
    stamped = _stamped(decoded, 0)
    for rows, name in ((_good(stamped), table), (_bad(stamped), quarantine_table)):
        if not spark.catalog.tableExists(name):
            spark.createDataFrame([], rows.schema).write.format("delta").saveAsTable(name)
            print(f"created {name}")
        ensure_retention(name)


# Time travel (reading a table as it was at an older version) needs the old data files. Delta's
# log keeps entries 30 days by default, but data files only 7 — and Predictive Optimization, on
# for every table here, runs VACUUM that deletes them after that. 30 days lines the two up.
# decisions.md, "Predictive Optimization stays on" (Session 7).
FILE_RETENTION = "interval 30 days"


def ensure_retention(table):
    """Set the time-travel window once. Checked first, because every ALTER is a new commit."""
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    if props.get("delta.deletedFileRetentionDuration") != FILE_RETENTION:
        spark.sql(
            f"ALTER TABLE {table} SET TBLPROPERTIES "
            f"('delta.deletedFileRetentionDuration' = '{FILE_RETENTION}')"
        )
        print(f"{table}: old data files now kept {FILE_RETENTION}")


def run_stream(
    *,
    stream,
    topic,
    subject,
    table,
    quarantine_table,
    checkpoint,
    app_id,
    idempotent=True,
    crash_after_batch=None,
):
    """Run one AvailableNow pass: read everything new, write it, stop. Progress is recorded
    even when the run fails, and the failure is re-raised."""
    decoded = decode(read_kafka(topic), subject)
    create_tables(decoded, table, quarantine_table)
    ensure_progress_table()
    query = (
        decoded.writeStream.foreachBatch(
            make_batch_writer(
                table,
                quarantine_table,
                app_id,
                idempotent=idempotent,
                crash_after_batch=crash_after_batch,
            )
        )
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    started = datetime.now(UTC)
    try:
        query.awaitTermination()
    finally:
        seconds = (datetime.now(UTC) - started).total_seconds()
        try:
            rows = record_progress(query, stream=stream, app_id=app_id)
            # Counted from offsets, NOT from Spark's numInputRows: on serverless that is 0 for
            # every foreachBatch batch, even one that wrote 50,000 rows (incidents.md,
            # 2026-09-27). With nothing new, Spark still files one report for the *next* batch
            # id whose start and end offsets are equal — a report, not a batch that ran.
            with_data = [r for r in rows if r["start_offsets"] != r["end_offsets"]]
            known = sum(r["rows_by_offsets"] for r in with_data if r["rows_by_offsets"] is not None)
            unknown = sum(1 for r in with_data if r["rows_by_offsets"] is None)
            note = f" (+ {unknown} first batch: Spark reports no start offset for it)" if unknown else ""
            print(
                f"{stream}: {len(rows)} progress report(s), {len(with_data)} batch(es) with data, "
                f"{known:,} rows by offsets{note}, {seconds:.0f}s"
            )
        except Exception:
            # Bookkeeping must not hide the real failure — and must not hide its own either:
            # the full traceback, not just the message (incidents.md, 2026-09-27).
            print(f"{stream}: could not record progress:")
            traceback.print_exc()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring — every batch's progress, kept in a table
# MAGIC
# MAGIC Spark keeps its Kafka position in the checkpoint, not in Kafka, so Kafka's own "consumer lag"
# MAGIC screens know nothing about this stream. The numbers come from Spark's progress reports instead.

# COMMAND ----------


PROGRESS_COLUMNS = (
    "run_id STRING, stream STRING, app_id STRING, query_id STRING, query_run_id STRING, "
    "batch_id BIGINT, batch_started_at STRING, num_input_rows BIGINT, "
    "processed_rows_per_second DOUBLE, duration_ms BIGINT, rows_by_offsets BIGINT, "
    "backlog_after_batch BIGINT, start_offsets STRING, end_offsets STRING, "
    "latest_offsets STRING, recorded_at STRING"
)


def ensure_progress_table():
    """Created before any stream starts, so checks that read it never meet a missing table."""
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {PROGRESS_TABLE} ("
        + PROGRESS_COLUMNS.replace("batch_started_at STRING", "batch_started_at TIMESTAMP").replace(
            "recorded_at STRING", "recorded_at TIMESTAMP"
        )
        + ")"
    )


def _as_dict(progress):
    """The report exactly as Spark's server sent it.

    Since PySpark 4.0 the report object is itself a dict subclass whose values PySpark has
    already converted (offsets as strings, a missing one as the text "null"). `.json` is the
    server's own JSON, the documented format, so read that whenever it exists.
    """
    return json.loads(progress.json) if hasattr(progress, "json") else dict(progress)


def _offsets(value):
    """'{"orders":{"0":5,"1":7}}' -> {"orders/0": 5, "orders/1": 7}.

    None when there is no offset. A stream's first batch has no start offset, and depending on
    the PySpark version that arrives as None, as JSON null, or as the four-letter text "null"
    (seen on Free Edition, PySpark 4.3.0.dev0). The text "null" is truthy and parses to None —
    which is what crashed the first run.
    """
    if value is None:
        return None
    parsed = json.loads(value) if isinstance(value, str) else value
    if parsed is None:
        return None
    return {f"{t}/{p}": int(o) for t, parts in parsed.items() for p, o in parts.items()}


def record_progress(query, *, stream, app_id):
    rows = []
    for progress in map(_as_dict, query.recentProgress):
        if not progress.get("sources"):
            continue
        source = progress["sources"][0]
        start = _offsets(source.get("startOffset"))
        end = _offsets(source.get("endOffset")) or {}
        latest = _offsets(source.get("latestOffset")) or {}
        rows.append(
            {
                "run_id": RUN_ID,
                "stream": stream,
                "app_id": app_id,
                "query_id": progress.get("id"),
                "query_run_id": progress.get("runId"),
                "batch_id": int(progress["batchId"]),
                "batch_started_at": progress.get("timestamp"),
                # Kept because it is what Spark reports — but on serverless foreachBatch it is 0
                # for every batch. Use rows_by_offsets.
                "num_input_rows": int(progress.get("numInputRows", 0)),
                "processed_rows_per_second": float(progress.get("processedRowsPerSecond") or 0.0),
                # NOT wall-clock time on this platform: bronze.inventory_cdc's 4 batches report
                # 93 + 166 + 162 + 99 = 520 s inside a run that took ~270 s. Unexplained; for
                # timing, use the Delta commit timestamps in DESCRIBE HISTORY.
                "duration_ms": int(progress.get("durationMs", {}).get("triggerExecution", 0)),
                # Independent of numInputRows: what the offsets say this batch covered.
                "rows_by_offsets": (sum(end[k] - start[k] for k in end) if start is not None else None),
                # How far behind the newest record this stream was when the batch finished.
                "backlog_after_batch": sum(latest[k] - end.get(k, 0) for k in latest),
                "start_offsets": json.dumps(start),
                "end_offsets": json.dumps(end),
                "latest_offsets": json.dumps(latest),
                "recorded_at": datetime.now(UTC).isoformat(),
            }
        )
    if rows:
        # Explicit column types: the first batch of a new stream has no start offset, so a
        # column can be all-None in one run and type inference would fail on it.
        (
            spark.createDataFrame([tuple(r.values()) for r in rows], PROGRESS_COLUMNS)
            .withColumn("batch_started_at", F.to_timestamp("batch_started_at"))
            .withColumn("recorded_at", F.to_timestamp("recorded_at"))
            .write.format("delta")
            .mode("append")
            .saveAsTable(PROGRESS_TABLE)
        )
    return rows


def check_backlog(stream):
    """Fails if the backlog left at the end of a run grew since the previous run.

    With AvailableNow a run reads everything that existed when it started, so what is left at
    the end is what arrived *during* the run. Growing run over run means the producer is
    outpacing the schedule. On today's fixed topics it is 0 every run — so this check cannot
    fail yet; recorded as built, not exercised.
    """
    ends = spark.sql(
        f"""
        SELECT run_id, max_by(backlog_after_batch, batch_id) AS end_backlog, max(recorded_at) AS at
        FROM {PROGRESS_TABLE} WHERE stream = '{stream}'
        GROUP BY run_id ORDER BY at DESC LIMIT 2
        """
    ).collect()
    print(f"{stream}: end-of-run backlog, newest first: {[r.end_backlog for r in ends]}")
    if len(ends) == 2:
        assert ends[0].end_backlog <= ends[1].end_backlog, f"{stream}: backlog is growing"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification — no duplicates, no gaps, nothing quarantined

# COMMAND ----------


def check_coordinates(table, quarantine_table, per_partition):
    """Every Kafka offset exactly once.

    Per partition: rows == distinct offsets (no duplicates) and rows == max - min + 1 (no gaps —
    offsets on these topics are contiguous: no transactions, no compaction). And the per-partition
    counts must equal what the laptop's read-only verifier counted on the topic itself.
    """
    got = {
        r._kafka_partition: r
        for r in spark.sql(
            f"""
            SELECT _kafka_partition,
                   count(*) AS n,
                   count(DISTINCT _kafka_offset) AS distinct_offsets,
                   max(_kafka_offset) - min(_kafka_offset) + 1 AS span
            FROM {table} GROUP BY _kafka_partition
            """
        ).collect()
    }
    for partition, expected in sorted(per_partition.items()):
        r = got.get(partition)
        line = (
            f"p{partition}: rows={r.n:,} distinct_offsets={r.distinct_offsets:,} span={r.span:,}"
            if r
            else f"p{partition}: MISSING"
        )
        ok = r is not None and r.n == r.distinct_offsets == r.span == expected
        print(f"{line}  expected={expected:,}  {'OK' if ok else 'MISMATCH'}")
        assert ok, f"{table} partition {partition}"
    assert set(got) == set(per_partition), f"{table}: unexpected partitions {sorted(got)}"
    quarantined = spark.table(quarantine_table).count()
    print(f"quarantined: {quarantined}")
    assert quarantined == 0, f"{quarantine_table} is not empty"


def check_batches_not_doubled(table, *, expect_equal=True):
    """Per batch: rows in the table vs distinct Kafka offsets that batch wrote.

    A batch written once has rows == distinct offsets. A batch written twice has exactly
    double. Reads only the table — the evidence is the data itself, not Spark's progress
    reports, which are monitoring and can be lost (they were, on the first orders run).
    exp_01's bug-first run passes expect_equal=False, because there the doubling is the point.
    """
    rows = spark.sql(
        f"""
        SELECT _batch_id AS batch_id, count(*) AS rows_in_table,
               count(DISTINCT _kafka_partition, _kafka_offset) AS distinct_offsets
        FROM {table} GROUP BY _batch_id ORDER BY _batch_id
        """
    ).collect()
    for r in rows:
        verdict = "OK" if r.rows_in_table == r.distinct_offsets else "WRITTEN MORE THAN ONCE"
        print(
            f"batch {r.batch_id}: rows {r.rows_in_table:,}, "
            f"distinct offsets {r.distinct_offsets:,}  {verdict}"
        )
    if expect_equal:
        doubled = [r.batch_id for r in rows if r.rows_in_table != r.distinct_offsets]
        assert rows and not doubled, f"{table}: batch(es) {doubled} written more than once"
    return rows


# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine self-test — drive the bad-record path on purpose
# MAGIC
# MAGIC The live topics hold zero bad records, so a normal run never exercises the quarantine
# MAGIC branch. Session 6's lesson: a green run can execute none of the code you care about. This
# MAGIC feeds `decode()` a real message and four broken copies of it, one at a time, and reports
# MAGIC what each became. Writes nothing.

# COMMAND ----------


def quarantine_self_test(table, subject):
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    sample = spark.table(table).select("_raw_value", "_kafka_key").limit(1).collect()[0]
    good = bytes(sample._raw_value)
    cases = {
        "real message": (sample._kafka_key, good),
        "truncated payload": (sample._kafka_key, good[:12]),
        "wrong magic byte": (sample._kafka_key, b"\x01" + good[1:]),
        "unknown schema id": (sample._kafka_key, b"\x00\x00\x0f\x42\x3f" + good[5:]),
        "null value": (sample._kafka_key, None),
        "missing key": (None, good),
    }
    kafka_shape = StructType(
        [
            StructField("key", BinaryType()),
            StructField("value", BinaryType()),
            StructField("topic", StringType()),
            StructField("partition", IntegerType()),
            StructField("offset", LongType()),
            StructField("timestamp", TimestampType()),
            StructField("timestampType", IntegerType()),
            StructField(
                "headers",
                ArrayType(StructType([StructField("key", StringType()), StructField("value", BinaryType())])),
            ),
        ]
    )
    # What each case must become. "unknown schema id" is left open on purpose: whether the
    # decoder looks the id up (and fails) or ignores it is exactly what is being found out.
    expected = {
        "real message": "decoded OK",
        "truncated payload": "avro decode failed",
        "wrong magic byte": "not Confluent wire format",
        "null value": "null value (tombstone)",
        "missing key": "missing key",
    }
    results = {}
    for name, (key, value) in cases.items():
        row = (key.encode() if key else None, value, "self-test", 0, 0, datetime.now(UTC), 0, [])
        try:
            out = decode(spark.createDataFrame([row], kafka_shape), subject).collect()[0]
            results[name] = out._quarantine_reason or "decoded OK"
            event_id = out._record.event_id if out._record is not None else None
        except Exception as e:  # the finding, if any case stops the batch instead of quarantining
            results[name] = f"RAISED {type(e).__name__}: {str(e)[:160]}"
            event_id = None
        want = expected.get(name)
        if want is None:
            verdict = "(open question)"
        else:
            verdict = "OK" if results[name] == want else f"EXPECTED {want}"
        print(f"{name:20} -> {results[name]:28} event_id={event_id}  {verdict}")
    wrong = {n: r for n, r in results.items() if n in expected and r != expected[n]}
    assert not wrong, f"quarantine routing is wrong for: {wrong}"
    return results
