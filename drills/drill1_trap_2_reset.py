# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Drill 1 — checkpoint-reset trap, part 2 of 2: delete the checkpoint, keep the app id
# MAGIC
# MAGIC **Run AFTER** part 1 and AFTER the two partial produces (74 new messages on `orders`).
# MAGIC
# MAGIC 1. **The trap.** Delete the checkpoint, keep app id `v1`, restart with the guard off.
# MAGIC    Expected: batches restart at 0; Delta skips 0 to 7 as "already written"; **the 74 new
# MAGIC    messages never land, and nothing errors.**
# MAGIC 2. **Name the lost messages** by reading exactly those offsets from Kafka. Expected: 74, all
# MAGIC    labelled by the drill's two runs, none in the table.
# MAGIC 3. **A normal restart** writes nothing: the checkpoint now says those offsets are done, so the
# MAGIC    loss is permanent from the stream's point of view.
# MAGIC 4. **The guard.** The same reset, and a "new generation into the old table", with the guard
# MAGIC    on. Expected: both refused before any stream starts.
# MAGIC 5. **The safe reset.** New generation (`v2` checkpoint + `v2` app id) into an empty table.
# MAGIC    Expected: every message once, the 74 included.
# MAGIC
# MAGIC Why step 1 loses data: "batch 7" covered 44,090 messages in life 1 and 44,164 in life 2. Delta
# MAGIC compares only the **number** (`txnVersion`), not the contents, so it cannot see that batch 7
# MAGIC changed. The unit of the guarantee is the batch, never the row (question bank B10).

# COMMAND ----------

# MAGIC %run ./_drill1_trap_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. The state life 1 left

# COMMAND ----------

BEFORE_END = table_end(TRAP_TABLE)
ROWS_BEFORE = spark.table(TRAP_TABLE).count()
COMMITS_BEFORE = data_commits(TRAP_TABLE)
print(f"table: {ROWS_BEFORE:,} rows, ends at {BEFORE_END}, {COMMITS_BEFORE} data commits")
print(f"checkpoint v1: commits {batch_files(CHECKPOINT_V1, 'commits')}")
assert ROWS_BEFORE == sum(BEFORE_END.values()), "life 1 did not leave a clean table — re-run part 1"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The trap — someone deletes the checkpoint; the app id stays `v1`

# COMMAND ----------

dbutils.fs.rm(CHECKPOINT_V1, True)
print(f"deleted {CHECKPOINT_V1}")

life2 = run_stream(**trap_args("drill1.trap.life2", CHECKPOINT_V1, APP_ID_V1), guard_reset=False)

LIFE2_END = topic_end(life2)
NEW_ON_TOPIC = sum(LIFE2_END.values()) - sum(BEFORE_END.values())
ran = [r for r in life2 if r["start_offsets"] != r["end_offsets"]]
rows_after = spark.table(TRAP_TABLE).count()
commits_after = data_commits(TRAP_TABLE)
last_end = {int(k.rsplit("/", 1)[1]): v for k, v in json.loads(ran[-1]["end_offsets"]).items()}

print(f"topic now ends at {LIFE2_END} — {NEW_ON_TOPIC} messages newer than life 1")
print(f"life 2 planned batches {[r['batch_id'] for r in ran]}; the last one ended at {last_end}")
print(f"table: {ROWS_BEFORE:,} rows before, {rows_after:,} after")
print(f"data commits: {COMMITS_BEFORE} before, {commits_after} after")

assert NEW_ON_TOPIC > 0, "no new messages on the topic — run the two partial produces first"
assert [r["batch_id"] for r in ran] == list(range(len(ran))), "life 2 should restart at batch 0"
assert last_end == LIFE2_END, "Spark should believe it processed the whole topic"
assert rows_after == ROWS_BEFORE, "expected the new messages NOT to land"
assert commits_after == COMMITS_BEFORE, "expected Delta to skip every batch (no new data commit)"
print(
    f"TRAP PRESENT: Spark processed batches 0-{ran[-1]['batch_id']} up to the end of the topic, "
    f"Delta skipped every one, {NEW_ON_TOPIC} messages lost, and no error anywhere."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Name the lost messages — read exactly those offsets from Kafka

# COMMAND ----------

starting = json.dumps({TOPIC: {str(p): o for p, o in BEFORE_END.items()}})
raw = (
    spark.read.format("kafka")
    .options(**kafka_options())
    .option("subscribe", TOPIC)
    .option("startingOffsets", starting)
    .option("endingOffsets", "latest")
    .option("includeHeaders", "true")
    .load()
)
LOST = (
    decode(raw, SUBJECT)
    .selectExpr(
        "_kafka_partition",
        "_kafka_offset",
        "_record.event_id AS event_id",
        f"{header_sql('ledgerline.run_id')} AS run_id",
        f"{header_sql('ledgerline.scope')} AS scope",
    )
    .collect()
)
# Collected (74 rows) and rebuilt from local values: part 5 drops the scratch table, and this list
# must outlive it. Explicit schema, because inference fails on a column that is all NULL.
spark.createDataFrame(
    [tuple(r) for r in LOST],
    "_kafka_partition INT, _kafka_offset BIGINT, event_id STRING, run_id STRING, scope STRING",
).createOrReplaceTempView("drill1_lost")
summary = spark.sql(
    f"""
    SELECT count(*) AS messages,
           count_if(l.run_id IS NOT NULL) AS labelled,
           count(DISTINCT l.run_id) AS runs,
           count_if(t.o IS NOT NULL) AS in_table,
           count_if(e.event_id IS NOT NULL) AS event_already_in_table
    FROM drill1_lost l
    LEFT JOIN (SELECT _kafka_partition AS p, _kafka_offset AS o FROM {TRAP_TABLE}) t
      ON t.p = l._kafka_partition AND t.o = l._kafka_offset
    LEFT JOIN (SELECT DISTINCT event_id FROM {TRAP_TABLE}) e ON e.event_id = l.event_id
    """
).collect()[0]
print(summary)
display(spark.sql("SELECT run_id, scope, count(*) AS messages FROM drill1_lost GROUP BY ALL"))
assert summary.messages == NEW_ON_TOPIC
assert summary.in_table == 0, "a 'lost' message is in the table after all"
assert summary.labelled == summary.messages, "every new message should carry the drill's run id"
# Here the loss is harmless only by luck: the drill produced DUPLICATES, so every lost event is
# already in the table once. A genuinely new order would have been lost outright.
print(
    f"{summary.messages} messages from {summary.runs} run(s) are on the topic and not in the table; "
    f"{summary.event_already_in_table} of their events happen to be there already (duplicates)."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. A normal restart does not repair it

# COMMAND ----------

run_stream(**trap_args("drill1.trap.restart", CHECKPOINT_V1, APP_ID_V1))
assert spark.table(TRAP_TABLE).count() == ROWS_BEFORE
print(
    "Restart wrote nothing: the checkpoint now records those offsets as done. From the stream's "
    "point of view the messages were processed — the loss is permanent until a rebuild."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The guard — both unsafe resets are refused before any stream starts

# COMMAND ----------

dbutils.fs.rm(CHECKPOINT_V1, True)
unsafe = {
    "checkpoint deleted, same app id (silent loss)": (CHECKPOINT_V1, APP_ID_V1),
    "new generation, old table (full duplicate)": (CHECKPOINT_V2, APP_ID_V2),
}
for label, (checkpoint, app_id) in unsafe.items():
    try:
        run_stream(**trap_args("drill1.trap.guarded", checkpoint, app_id))
    except UnsafeStreamReset as e:
        print(f"REFUSED  {label}\n         {e}\n")
    else:
        raise AssertionError(f"the guard let this through: {label}")
assert spark.table(TRAP_TABLE).count() == ROWS_BEFORE
assert batch_files(CHECKPOINT_V2, "offsets") == [], "a refused run must not have started a stream"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The safe reset — new generation, into an empty table (a rebuild)

# COMMAND ----------

drop_scratch()
rebuilt = run_stream(**trap_args("drill1.trap.rebuild", CHECKPOINT_V2, APP_ID_V2))
REBUILT_END = topic_end(rebuilt)
check_coordinates(TRAP_TABLE, TRAP_QUARANTINE, REBUILT_END)
check_batches_not_doubled(TRAP_TABLE)
missing = spark.sql(
    f"""
    SELECT count(*) AS n FROM drill1_lost l
    LEFT ANTI JOIN {TRAP_TABLE} t
      ON t._kafka_partition = l._kafka_partition AND t._kafka_offset = l._kafka_offset
    """
).collect()[0].n
assert missing == 0
print(f"FIXED: {sum(REBUILT_END.values()):,} rows, every offset once, all {len(LOST)} lost messages present.")

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {TRAP_TABLE}")
    .select("version", "timestamp", "operation", "operationMetrics.numOutputRows")
    .orderBy("version")
)