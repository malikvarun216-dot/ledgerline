# Databricks notebook source
# MAGIC %md
# MAGIC # exp_01 — exactly-once replay: kill the job after Delta commits, before Spark records it
# MAGIC
# MAGIC **The one window that matters.** A micro-batch does two things in order: (1) writes its rows
# MAGIC to Delta, (2) tells the checkpoint "batch N is done". A crash between the two leaves the rows
# MAGIC in the table while the checkpoint still says batch N never finished — so the restart runs
# MAGIC batch N again, with the same offsets and the same rows.
# MAGIC
# MAGIC Run twice, identically, except for one thing:
# MAGIC
# MAGIC | Variant | Write options | Expected after crash + restart |
# MAGIC |---|---|---|
# MAGIC | `bug` | plain append | batch N is in the table **twice** — no error anywhere |
# MAGIC | `fix` | append + `txnAppId` / `txnVersion = batch_id` | every offset exactly once |
# MAGIC
# MAGIC The bug is asserted *present* first, then the fix is asserted to remove it. Both runs use the
# MAGIC same shared writer as Bronze (`databricks/bronze/_kafka_bronze`), so the only difference
# MAGIC under test is the two write options.
# MAGIC
# MAGIC **Scratch tables only** (`workspace.bronze.exp01_*`), rebuilt on every run. The real Bronze
# MAGIC tables are never touched. Reads the live `orders` topic, which is read-only here.

# COMMAND ----------

# MAGIC %run ../databricks/bronze/_kafka_bronze

# COMMAND ----------

TOPIC = "orders"
SUBJECT = "orders-value"
TOTAL = 394_090
CRASH_AFTER = 2
EXP_STATE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/exp_01"


def fresh(variant):
    """A brand-new scratch table, quarantine table and checkpoint for one variant."""
    table = f"{CATALOG}.{SCHEMA}.exp01_{variant}"
    quarantine = f"{table}_quarantine"
    checkpoint = f"{EXP_STATE}/{variant}"
    for name in (table, quarantine):
        spark.sql(f"DROP TABLE IF EXISTS {name}")
    dbutils.fs.rm(checkpoint, True)
    return {
        "stream": f"exp01.{variant}",
        "topic": TOPIC,
        "subject": SUBJECT,
        "table": table,
        "quarantine_table": quarantine,
        "checkpoint": checkpoint,
        "app_id": f"exp01.{variant}.{RUN_ID}",
    }


def crash_then_resume(variant, idempotent):
    args = fresh(variant)
    try:
        run_stream(**args, idempotent=idempotent, crash_after_batch=CRASH_AFTER)
    except Exception as e:
        assert "SIMULATED CRASH" in str(e), f"{variant}: crashed for the wrong reason: {e}"
        print(f"{variant}: crashed as planned, right after batch {CRASH_AFTER} reached Delta")
    else:
        raise AssertionError(f"{variant}: the simulated crash never happened")

    landed_before_restart = spark.table(args["table"]).count()
    run_stream(**args, idempotent=idempotent)

    s = spark.sql(
        f"""
        SELECT count(*) AS rows,
               count(DISTINCT _kafka_partition, _kafka_offset) AS distinct_offsets,
               count_if(_batch_id = {CRASH_AFTER}) AS rows_stamped_crashed_batch
        FROM {args['table']}
        """
    ).collect()[0]
    sizes = {
        r.batch_id: r.n
        for r in spark.sql(
            f"""SELECT batch_id, max(num_input_rows) AS n FROM {PROGRESS_TABLE}
                WHERE app_id = '{args['app_id']}' GROUP BY batch_id"""
        ).collect()
    }
    crashed_batch = sizes[CRASH_AFTER]
    result = {
        "variant": variant,
        "rows": s.rows,
        "distinct_offsets": s.distinct_offsets,
        "duplicate_rows": s.rows - s.distinct_offsets,
        "crashed_batch_size": crashed_batch,
        "rows_stamped_crashed_batch": s.rows_stamped_crashed_batch,
        "landed_before_restart": landed_before_restart,
        "batches_0_to_crash": sum(sizes[b] for b in range(CRASH_AFTER + 1)),
    }
    print(result)
    # The crash happened in the right window: batch N's rows were already in Delta.
    assert result["landed_before_restart"] == result["batches_0_to_crash"], "crash was not after the commit"
    # Nothing lost in either variant.
    assert result["distinct_offsets"] == TOTAL
    check_batches_not_doubled(args["table"], args["app_id"], expect_equal=idempotent)
    return result, args


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The bug — plain append, crash, restart

# COMMAND ----------

bug, bug_args = crash_then_resume("bug", idempotent=False)
assert bug["duplicate_rows"] == bug["crashed_batch_size"] > 0, "expected the crashed batch to be doubled"
assert bug["rows"] == TOTAL + bug["crashed_batch_size"]
assert bug["rows_stamped_crashed_batch"] == 2 * bug["crashed_batch_size"]
print(f"BUG PRESENT: {bug['duplicate_rows']:,} duplicate rows, and the job reported success.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The fix — same crash, same restart, with `txnAppId` / `txnVersion`

# COMMAND ----------

fix, fix_args = crash_then_resume("fix", idempotent=True)
assert fix["duplicate_rows"] == 0
assert fix["rows"] == TOTAL
assert fix["rows_stamped_crashed_batch"] == fix["crashed_batch_size"]
print(f"FIX HOLDS: {fix['rows']:,} rows, 0 duplicates; Delta skipped the replay of batch {CRASH_AFTER}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Look — the replayed batch is a second commit in one history and absent from the other

# COMMAND ----------

for args in (bug_args, fix_args):
    print(args["table"])
    display(
        spark.sql(f"DESCRIBE HISTORY {args['table']}")
        .select("version", "timestamp", "operation", "operationParameters", "operationMetrics.numOutputRows")
        .orderBy("version")
    )
