# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # exp_01 — exactly-once replay: a crash after Delta commits, before Spark records it
# MAGIC
# MAGIC **The one window that matters.** A micro-batch does two things in order: (1) writes its rows
# MAGIC to Delta, (2) writes "batch N is done" into the checkpoint (`commits/N`). The checkpoint also
# MAGIC holds `offsets/N` — which Kafka offsets batch N covers — written *before* the batch runs.
# MAGIC A crash between (1) and (2) leaves exactly this on disk:
# MAGIC
# MAGIC | | batch N |
# MAGIC |---|---|
# MAGIC | rows in the Delta table | **there** |
# MAGIC | `offsets/N` in the checkpoint | **there** |
# MAGIC | `commits/N` in the checkpoint | **missing** |
# MAGIC
# MAGIC On restart Spark sees offsets without a commit and runs batch N again — same offsets, same rows.
# MAGIC
# MAGIC **How the crash is reproduced.** Each variant loads the whole topic cleanly, then deletes the
# MAGIC last `commits/N` file — which leaves precisely the state in the table above — and restarts.
# MAGIC An earlier version raised an exception inside the batch instead; it produced the same
# MAGIC doubling (49,999 rows), but Databricks marks a notebook command failed whenever a stream
# MAGIC started in it dies — even when the code catches the error — so "Run all" stopped there
# MAGIC (incidents.md, 2026-09-27). Recreating the *state* a crash leaves, rather than staging the
# MAGIC crash itself, is deterministic and involves no failed stream.
# MAGIC
# MAGIC | Variant | Write options | Expected after the replay |
# MAGIC |---|---|---|
# MAGIC | `bug` | plain append | batch N is in the table **twice** — no error anywhere |
# MAGIC | `fix` | append + `txnAppId` / `txnVersion = batch_id` | every offset exactly once |
# MAGIC
# MAGIC The bug is asserted *present* first, then the fix is asserted to remove it. Both use the shared
# MAGIC Bronze writer (`databricks/bronze/_kafka_bronze`), so the only difference is the two options.
# MAGIC And both assert that the restart **really re-ran batch N** — otherwise "0 duplicates" in the
# MAGIC fix could just mean nothing ran.
# MAGIC
# MAGIC **Scratch tables only** (`workspace.bronze.exp01_*`), rebuilt every run. The real Bronze tables
# MAGIC are never touched. Reads the live `orders` topic, read-only.

# COMMAND ----------

# MAGIC %run ../databricks/bronze/_kafka_bronze

# COMMAND ----------

TOPIC = "orders"
SUBJECT = "orders-value"
# The topic's size is read from the broker on each load (topic_end), not written here: it was
# 394,090 until Drill 1 added 74 labelled duplicates, and a frozen total would have failed.
EXP_STATE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/exp_01"


def fresh(variant):
    """A brand-new scratch table, quarantine table and checkpoint for one variant."""
    table = f"{CATALOG}.{SCHEMA}.exp01_{variant}"
    quarantine = f"{table}_quarantine"
    checkpoint = f"{EXP_STATE}/{variant}"
    for name in (table, quarantine):
        spark.sql(f"DROP TABLE IF EXISTS {name}")
    try:
        dbutils.fs.rm(checkpoint, True)
    except Exception as e:  # the very first run has no checkpoint to remove — anything else is real
        if "not found" not in str(e).lower() and "FileNotFound" not in type(e).__name__:
            raise
    return {
        "stream": f"exp01.{variant}",
        "topic": TOPIC,
        "subject": SUBJECT,
        "table": table,
        "quarantine_table": quarantine,
        "checkpoint": checkpoint,
        "app_id": f"exp01.{variant}.{RUN_ID}",
    }


def batch_files(checkpoint, kind):
    """Batch numbers that have a file in <checkpoint>/<kind>/ ("offsets" or "commits")."""
    return sorted(int(f.name) for f in dbutils.fs.ls(f"{checkpoint}/{kind}") if f.name.isdigit())


def load_then_replay_last_batch(variant, idempotent):
    args = fresh(variant)

    # 1. A clean, complete load.
    total = sum(topic_end(run_stream(**args, idempotent=idempotent)).values())
    offsets, commits = batch_files(args["checkpoint"], "offsets"), batch_files(args["checkpoint"], "commits")
    last = commits[-1]
    print(f"{variant}: checkpoint after the load — offsets {offsets}, commits {commits}")
    assert offsets == commits == list(range(last + 1)), "checkpoint not in the expected shape"
    assert spark.table(args["table"]).count() == total

    # 2. Leave the state a crash would: batch `last` is in Delta, its offsets are logged,
    #    its "done" marker is not.
    dbutils.fs.rm(f"{args['checkpoint']}/commits/{last}")
    print(f"{variant}: removed commits/{last} — batch {last} now looks unfinished to Spark")

    # 3. Restart. Spark must re-run batch `last` with the offsets it logged.
    replay = run_stream(**args, idempotent=idempotent)
    reran = [r for r in replay if r["start_offsets"] != r["end_offsets"]]
    print(f"{variant}: restart re-ran batch(es) {[r['batch_id'] for r in reran]}")
    assert [r["batch_id"] for r in reran] == [last], f"{variant}: the restart did not re-run batch {last}"

    s = spark.sql(
        f"""
        SELECT count(*) AS rows,
               count(DISTINCT _kafka_partition, _kafka_offset) AS distinct_offsets,
               count_if(_batch_id = {last}) AS rows_stamped_replayed_batch,
               count(DISTINCT CASE WHEN _batch_id = {last}
                          THEN concat(_kafka_partition, ':', _kafka_offset) END) AS replayed_batch_size
        FROM {args['table']}
        """
    ).collect()[0]
    result = {
        "variant": variant,
        "total": total,
        "replayed_batch": last,
        "rows": s.rows,
        "distinct_offsets": s.distinct_offsets,
        "duplicate_rows": s.rows - s.distinct_offsets,
        "replayed_batch_size": s.replayed_batch_size,
        "replay_rows_by_offsets": reran[0]["rows_by_offsets"],
        "rows_stamped_replayed_batch": s.rows_stamped_replayed_batch,
    }
    print(result)
    # The replay covered exactly the batch's own offsets — the same rows, not new ones.
    assert result["replay_rows_by_offsets"] == result["replayed_batch_size"]
    # Nothing lost in either variant.
    assert result["distinct_offsets"] == total
    check_batches_not_doubled(args["table"], expect_equal=idempotent)
    return result, args


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The bug — plain append, then the replay

# COMMAND ----------

bug, bug_args = load_then_replay_last_batch("bug", idempotent=False)
assert bug["duplicate_rows"] == bug["replayed_batch_size"] > 0, "expected the replayed batch to be doubled"
assert bug["rows"] == bug["total"] + bug["replayed_batch_size"]
assert bug["rows_stamped_replayed_batch"] == 2 * bug["replayed_batch_size"]
print(f"BUG PRESENT: {bug['duplicate_rows']:,} duplicate rows, and the job reported success.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The fix — same replay, with `txnAppId` / `txnVersion`

# COMMAND ----------

fix, fix_args = load_then_replay_last_batch("fix", idempotent=True)
assert fix["duplicate_rows"] == 0
assert fix["rows"] == fix["total"]
assert fix["rows_stamped_replayed_batch"] == fix["replayed_batch_size"]
print(
    f"FIX HOLDS: batch {fix['replayed_batch']} re-ran, and Delta skipped its write — "
    f"{fix['rows']:,} rows, 0 duplicates."
)

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