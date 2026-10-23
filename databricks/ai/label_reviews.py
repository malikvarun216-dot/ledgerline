# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # AI review labels — the production step (Session 22)
# MAGIC
# MAGIC Each distinct review message in `silver.review` gets a **sentiment** (positive / neutral / negative)
# MAGIC and a **topic** (delivery / product / seller / price / other) from Llama 3.3 70B through `ai_query`.
# MAGIC The design is Session 19b's, measured there (decisions.md, "An AI label is stored like an event"):
# MAGIC
# MAGIC - **`silver.review_ai_calls`** — every call ever made, append-only, the reply as received.
# MAGIC - **`silver.review_labels`** — one row per `(input_hash, model, prompt_version)`, filled by a MERGE
# MAGIC   that only INSERTs. A text already answered is never asked again — so a re-run, a replay of every
# MAGIC   night, or a second copy of the same message costs no call and changes no label.
# MAGIC - An **error** is logged, not labelled: the next run asks again. A **bad answer** (outside the label
# MAGIC   set) is kept with `label_ok = false`: asking again at temperature 0 mostly repeats it.
# MAGIC - The text is trimmed of white space at both ends **before** hashing; a message that is only white
# MAGIC   space is never sent (incidents.md 2026-10-21: a lone `\n` came back "negative, product").
# MAGIC - At most `max_calls` texts per run, oldest review first — the first fill (~34,000 texts) is spread
# MAGIC   over several runs on purpose; each run is complete on its own.
# MAGIC
# MAGIC **Session 19b's answers are carried over once** (`workspace.s19b.review_labels`, the 70B / v1 rows,
# MAGIC 1,500 texts): an answer already paid for is history, not a cache to rebuild.
# MAGIC
# MAGIC The labels reach Snowflake through the bridge like any Silver table (`review_labels` in its
# MAGIC `TABLES`); Gold joins them to `fct_reviews` on `message_hash`. **Run with `mode` = `plan` first.**

# COMMAND ----------

import time

from pyspark.sql import functions as F

SILVER = "workspace.silver"
REVIEWS = f"{SILVER}.review"
CALLS = f"{SILVER}.review_ai_calls"
LABELS = f"{SILVER}.review_labels"
S19B_LABELS = "workspace.s19b.review_labels"

# The pinned pair. Changing either is a new key — a deliberate, measured backfill, never a silent
# rewrite. dbt's Gold model review_labels reads the same pair (vars ai_label_model / _prompt_version).
MODEL = "databricks-meta-llama-3-3-70b-instruct"
PROMPT_VERSION = "v1"
PROMPT = (
    "You label Brazilian e-commerce customer reviews written in Portuguese. Reply with exactly two "
    "lowercase English words separated by a comma, and nothing else: the sentiment (positive, neutral "
    "or negative), then the main topic (delivery, product, seller, price or other). Review: "
)
SENTIMENTS = ("positive", "neutral", "negative")
TOPICS = ("delivery", "product", "seller", "price", "other")
ANSWER = rf"^({'|'.join(SENTIMENTS)}),({'|'.join(TOPICS)})$"

# Change Data Feed: the bridge exports the label table by its feed, like every Silver table.
PROPS = "'delta.enableChangeDataFeed' = 'true', 'delta.deletedFileRetentionDuration' = 'interval 30 days'"

dbutils.widgets.dropdown("mode", "plan", ["plan", "label"])
dbutils.widgets.text("max_calls", "5000")
MODE = dbutils.widgets.get("mode")
MAX_CALLS = int(dbutils.widgets.get("max_calls"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables (created once; safe to re-run)

# COMMAND ----------

spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {CALLS} (batch_id INT, purpose STRING, input_hash STRING, model STRING,
        prompt_version STRING, temperature DOUBLE, response STRING, error STRING, called_at TIMESTAMP)"""
)
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {LABELS} (input_hash STRING NOT NULL, model STRING NOT NULL,
        prompt_version STRING NOT NULL, sentiment STRING, topic STRING, label_ok BOOLEAN,
        response STRING, called_at TIMESTAMP, batch_id INT) TBLPROPERTIES ({PROPS})"""
)


def texts():
    """One row per distinct trimmed message in Silver: its hash, the text, how many reviews carry it,
    and the oldest of those reviews (the order in which texts are asked).

    Not F.trim: Spark's trim removes only spaces. No "(?U)" flag: on Databricks it means UNGREEDY.
    Plain \\s is Java's six white-space characters — Gold trims with [[:space:]], the same six (S22)."""
    return (
        spark.table(REVIEWS)
        .select(
            F.regexp_replace("review_comment_message", r"^\s+|\s+$", "").alias("text"),
            "review_creation_date",
        )
        .where(F.col("text") != "")
        .withColumn("input_hash", F.sha2("text", 256))
        .groupBy("input_hash")
        .agg(
            F.any_value("text").alias("text"),
            F.count("*").alias("reviews"),
            F.min("review_creation_date").alias("first_review"),
        )
    )


def todo():
    """Texts with no answer yet under the pinned (model, prompt_version), oldest first, capped."""
    done = spark.table(LABELS).where(
        (F.col("model") == MODEL) & (F.col("prompt_version") == PROMPT_VERSION)
    )
    return (
        texts().join(done, "input_hash", "left_anti")
        .orderBy("first_review", "input_hash")
        .limit(MAX_CALLS)
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Carry Session 19b's answers over (once — an insert-only MERGE, so a re-run adds nothing)

# COMMAND ----------

# Only answers whose text is in Silver BYTE FOR BYTE (its hash is one of texts()). The first run carried
# all 1,500 and 101 matched nothing: S19b read the raw Olist file, whose lines end in \r\n, and Spark's
# CSV reader turned a line break INSIDE a message into \n; our landed file ends lines in \n, so Silver
# keeps \r\n. 2,678 distinct texts differ that way (incidents.md 2026-10-24). An answer to a different
# string is not an answer to this one.
carried = 0
if spark.catalog.tableExists(S19B_LABELS):
    s19b = spark.table(S19B_LABELS).where(
        (F.col("model") == MODEL) & (F.col("prompt_version") == PROMPT_VERSION)
    )
    s19b_here = s19b.join(texts().select("input_hash"), "input_hash", "left_semi")
    new = s19b_here.join(spark.table(LABELS).select("input_hash", "model", "prompt_version"),
                         ["input_hash", "model", "prompt_version"], "left_anti")
    carried = new.count()
    print(f"S19b answers for {MODEL} / {PROMPT_VERSION}: {s19b.count():,}; their text in Silver: "
          f"{s19b_here.count():,}; not yet here: {carried:,}")
    if MODE == "label" and carried:
        s19b_here.createOrReplaceTempView("_s19b")
        spark.sql(
            f"""MERGE INTO {LABELS} t
                USING (SELECT input_hash, model, prompt_version, sentiment, topic, label_ok, response,
                              called_at, batch_id FROM _s19b) s
                ON t.input_hash = s.input_hash AND t.model = s.model AND t.prompt_version = s.prompt_version
                WHEN NOT MATCHED THEN INSERT *"""
        )
else:
    print(f"{S19B_LABELS} not found — nothing to carry over")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove what the first carry wrongly inserted (2026-10-24; a no-op once done)
# MAGIC
# MAGIC A pinned answer whose hash is neither a Silver text nor ever asked in production can only have come
# MAGIC from the unfiltered carry: 101 rows, each the answer to a string Silver does not hold. They are
# MAGIC deleted, not left: kept, they would fail Gold's join-back test forever and stand for texts that do
# MAGIC not exist. Their texts are asked again under their real hash (101 calls, $0). Time travel keeps them.

# COMMAND ----------

asked = spark.table(CALLS).where("purpose = 'production'").select("input_hash").distinct()
wrong = (
    spark.table(LABELS).where((F.col("model") == MODEL) & (F.col("prompt_version") == PROMPT_VERSION))
    .join(texts().select("input_hash"), "input_hash", "left_anti")
    .join(asked, "input_hash", "left_anti")
    .select("input_hash", "model", "prompt_version")
)
n_wrong = wrong.count()
print(f"carried answers matching no Silver text: {n_wrong:,}")
if MODE == "label" and n_wrong:
    wrong.createOrReplaceTempView("_wrong")
    spark.sql(
        f"""MERGE INTO {LABELS} t USING _wrong s
            ON t.input_hash = s.input_hash AND t.model = s.model AND t.prompt_version = s.prompt_version
            WHEN MATCHED THEN DELETE"""
    )
    print(f"deleted {n_wrong:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plan: what this run would ask

# COMMAND ----------

pinned = spark.table(LABELS).where((F.col("model") == MODEL) & (F.col("prompt_version") == PROMPT_VERSION))
n_texts = texts().count()
n_labelled = texts().join(pinned, "input_hash", "left_semi").count()
waiting = n_texts - n_labelled
print(f"distinct texts in Silver: {n_texts:,}; answered under {MODEL} / {PROMPT_VERSION}: {n_labelled:,}; "
      f"waiting: {waiting:,}; this run asks: {min(waiting, MAX_CALLS):,} (max_calls = {MAX_CALLS:,})")
print(f"mode = {MODE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Label (only with `mode` = `label`)
# MAGIC
# MAGIC The calls are made by ONE `INSERT … SELECT ai_query(…)` into the call log, so each text is asked
# MAGIC exactly once: a DataFrame holding `ai_query`, read twice, would call the model twice (S19b). The
# MAGIC answers then go into the label table by the insert-only MERGE, from this batch's log rows only.

# COMMAND ----------

result = {"batch": None, "calls": 0, "errors": 0, "bad": 0, "seconds": 0.0}
if MODE == "label" and waiting:
    todo().select("input_hash", "text").createOrReplaceTempView("_todo")
    batch = spark.sql(f"SELECT coalesce(max(batch_id), 0) + 1 AS b FROM {CALLS}").first().b
    started = time.time()
    spark.sql(
        f"""INSERT INTO {CALLS}
            SELECT {batch}, 'production', input_hash, '{MODEL}', '{PROMPT_VERSION}', CAST(0.0 AS DOUBLE),
                   r.result, r.errorMessage, current_timestamp()
            FROM (
              SELECT input_hash,
                     ai_query('{MODEL}', concat(:prompt, text), failOnError => false,
                              modelParameters => named_struct('temperature', CAST(0.0 AS DOUBLE))) AS r
              FROM _todo
            )""",
        args={"prompt": PROMPT},
    )
    seconds = round(time.time() - started, 1)
    spark.sql(
        f"""MERGE INTO {LABELS} t
            USING (
              SELECT input_hash, model, prompt_version, response,
                     regexp_replace(lower(response), '\\\\s', '') AS answer, called_at, batch_id
              FROM {CALLS}
              WHERE batch_id = {batch} AND error IS NULL AND response IS NOT NULL
              QUALIFY row_number() OVER (
                PARTITION BY input_hash, model, prompt_version ORDER BY called_at) = 1
            ) s
            ON t.input_hash = s.input_hash AND t.model = s.model AND t.prompt_version = s.prompt_version
            WHEN NOT MATCHED THEN INSERT (input_hash, model, prompt_version, sentiment, topic, label_ok,
                                          response, called_at, batch_id)
              VALUES (s.input_hash, s.model, s.prompt_version,
                      nullif(regexp_extract(s.answer, '{ANSWER}', 1), ''),
                      nullif(regexp_extract(s.answer, '{ANSWER}', 2), ''),
                      s.answer RLIKE '{ANSWER}', s.response, s.called_at, s.batch_id)"""
    )
    log = spark.table(CALLS).where(F.col("batch_id") == batch)
    result = {
        "batch": batch,
        "calls": log.count(),
        "errors": log.where("error IS NOT NULL").count(),
        "bad": spark.table(LABELS).where((F.col("batch_id") == batch) & ~F.col("label_ok")).count(),
        "seconds": seconds,
    }
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — one answer per key, every answered text still in Silver, nothing asked twice

# COMMAND ----------

labels = spark.table(LABELS)
keys = labels.select("input_hash", "model", "prompt_version")
assert labels.count() == keys.distinct().count(), "two answers for one (input_hash, model, prompt_version)"
pinned = labels.where((F.col("model") == MODEL) & (F.col("prompt_version") == PROMPT_VERSION))
orphans = pinned.join(texts(), "input_hash", "left_anti").count()
asked_twice = (
    spark.table(CALLS).where("purpose = 'production' AND error IS NULL")
    .groupBy("input_hash", "model", "prompt_version").count().where("count > 1").count()
)
summary = pinned.groupBy("label_ok").count().collect()
print(f"labels: {labels.count():,} ({pinned.count():,} pinned); by label_ok: {[tuple(r) for r in summary]}; "
      f"answers whose text is no longer in Silver: {orphans:,}; answered twice in production: {asked_twice}")
assert asked_twice == 0, "a text was answered twice: the todo list or the MERGE let one through"
display(pinned.groupBy("sentiment", "topic").count().orderBy(F.desc("count")))