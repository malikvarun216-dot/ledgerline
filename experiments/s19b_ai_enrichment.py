# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # An AI step inside a pipeline, built to replay like every other layer (Session 19b)
# MAGIC
# MAGIC **The step:** free text in, structured columns out. Each Olist review comment (Portuguese) gets
# MAGIC a `sentiment` (positive / neutral / negative) and a `topic` (delivery / product / seller / price /
# MAGIC other) from a model called through `ai_query` — a SQL function that sends one prompt per row to a
# MAGIC model served by Databricks and returns its reply.
# MAGIC
# MAGIC **The problem:** every other derived column in this project is a pure function of its input — re-run
# MAGIC it and you get the same value. A model is not: the same prompt can come back with a different answer,
# MAGIC and next month's model is a different function. Recompute the column on every run and a backfill
# MAGIC silently rewrites history. So the answer is treated like an event, not like a formula:
# MAGIC
# MAGIC - **`ai_calls`** — every call ever made, append-only (the Bronze of this step): the reply as received.
# MAGIC - **`review_labels`** — one row per `(input_hash, model, prompt_version)`, filled by a MERGE that
# MAGIC   only ever INSERTs (the Silver of this step). A key that already has an answer is never asked again.
# MAGIC - A **transient failure** (endpoint error, rate limit) is logged but not labelled, so the next run
# MAGIC   retries it. A **bad answer** (not one of the allowed labels) is labelled `label_ok = false` and
# MAGIC   kept: asking again at temperature 0 would mostly repeat it; fixing it is a new prompt version.
# MAGIC - Changing the model or the prompt is a **new key**, so it is a deliberate, versioned backfill; the
# MAGIC   old answers stay. Readers see one pinned `(model, prompt_version)` through a view.
# MAGIC
# MAGIC Scratch in `workspace.s19b` (the S11 rule). The text is Olist's `order_reviews`, read from Drill 1's
# MAGIC scratch copy in S3 — an experiment table, not a fourth source (decisions.md, 2026-10-21).
# MAGIC
# MAGIC Cases and predictions:
# MAGIC - **A** load: 99,224 review records (needs `multiLine` + `escape='"'`, constraints S7), 40,950 with
# MAGIC   text, **35,616 distinct texts** after trimming — keying on the text's hash saves 5,334 calls.
# MAGIC - **B** run 1, 1,000 texts, Llama 3.3 70B at temperature 0: 1,000 calls, 1,000 label rows. Bad
# MAGIC   answers and errors counted, not assumed (predicted: a few bad, 0 errors).
# MAGIC - **C** run 1 again, unchanged: **0 calls**, label table unchanged to the row.
# MAGIC - **D** the sample grows to 1,500: **500 calls**; the first 1,000 rows unchanged.
# MAGIC - **E** repeatability: 200 texts asked 5 times at temperature 0, and 5 times at 1.0. Texts whose
# MAGIC   answer changed at least once — measured (predicted: few at 0, not certainly none; many more at 1.0).
# MAGIC - **F** model change: Llama 3.1 8B on the same 1,500 → 1,500 rows under a new key, the 70B rows
# MAGIC   untouched, the pinned view unchanged. Agreement between the two models — measured.
# MAGIC - **G** graded: sentiment against the customer's own star score (1-2 negative, 3 neutral, 4-5
# MAGIC   positive), per review record. The topic has no answer key — only agreement can be measured.
# MAGIC - **H** `ai_classify` (no model named) against `ai_query`'s topic on the 200 texts — measured.

# COMMAND ----------

import time

from pyspark.sql import functions as F

S = "workspace.s19b"
SRC_CSV = "s3://ledgerline-landing-dev-fffc8b65/ledgerline/drill1/reviews/reviews.csv"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S} COMMENT 'Session 19b scratch: AI enrichment experiment'")

REVIEWS = f"{S}.reviews"            # one row per review record
TEXTS = f"{S}.review_texts"         # one row per distinct trimmed text, keyed by its sha256
CALLS = f"{S}.ai_calls"             # every model call, append-only
LABELS = f"{S}.review_labels"       # one answer per (input_hash, model, prompt_version), insert-only
CURRENT = f"{S}.review_labels_current"  # the pinned (model, prompt_version) readers see
CLASSIFY = f"{S}.ai_classify_topic"

BIG = "databricks-meta-llama-3-3-70b-instruct"
SMALL = "databricks-meta-llama-3-1-8b-instruct"
PROMPT_VERSION = "v1"
PROMPT = (
    "You label Brazilian e-commerce customer reviews written in Portuguese. Reply with exactly two "
    "lowercase English words separated by a comma, and nothing else: the sentiment (positive, neutral "
    "or negative), then the main topic (delivery, product, seller, price or other). Review: "
)
SENTIMENTS = ("positive", "neutral", "negative")
TOPICS = ("delivery", "product", "seller", "price", "other")
ANSWER = rf"^({'|'.join(SENTIMENTS)}),({'|'.join(TOPICS)})$"

N1, N2 = 1_000, 1_500
N_REPEAT, REPEATS = 200, 5

for t in (CURRENT,):
    spark.sql(f"DROP VIEW IF EXISTS {t}")
for t in (CLASSIFY, LABELS, CALLS, TEXTS, REVIEWS):
    spark.sql(f"DROP TABLE IF EXISTS {t}")


def history(table):
    return spark.sql(f"DESCRIBE HISTORY {table}").orderBy("version").collect()


def merges(table):
    return [h for h in history(table) if h.operation == "MERGE"]


def fingerprint(table, where="true"):
    """Row count and an order-free hash of every row: equal fingerprints = the same rows."""
    r = spark.sql(
        f"SELECT count(*) AS n, coalesce(sum(xxhash64(*)), 0) AS h FROM {table} WHERE {where}"
    ).first()
    return (r.n, r.h)


def sample_view(name, n):
    """The first n distinct texts by hash: deterministic, and a larger n contains every smaller one."""
    spark.sql(
        f"CREATE OR REPLACE TEMP VIEW {name} AS "
        f"SELECT input_hash, text FROM {TEXTS} ORDER BY input_hash LIMIT {n}"
    )


def call_model(todo, model, temperature, purpose):
    """One model call per row of `todo`, appended to the call log in ONE query, so each row is asked
    exactly once (a DataFrame holding ai_query, read twice, would call the model twice). Returns
    (batch_id, calls, seconds)."""
    batch = spark.sql(f"SELECT coalesce(max(batch_id), 0) + 1 AS b FROM {CALLS}").first().b
    started = time.time()
    spark.sql(
        f"""INSERT INTO {CALLS}
            SELECT {batch} AS batch_id, '{purpose}' AS purpose, input_hash, '{model}' AS model,
                   '{PROMPT_VERSION}' AS prompt_version, CAST({temperature} AS DOUBLE) AS temperature,
                   r.result AS response, r.errorMessage AS error, current_timestamp() AS called_at
            FROM (
              SELECT input_hash,
                     ai_query('{model}', concat(:prompt, text),
                              failOnError => false,
                              modelParameters => named_struct(
                                'temperature', CAST({temperature} AS DOUBLE))) AS r
              FROM {todo}
            )""",
        args={"prompt": PROMPT},
    )
    seconds = round(time.time() - started, 1)
    calls = spark.table(CALLS).where(F.col("batch_id") == batch).count()
    return batch, calls, seconds


def enrich(n, model):
    """The production-shaped step: ask only for keys with no answer yet, then MERGE the new answers in.
    A transient failure (error) is not labelled, so the next run asks again."""
    sample_view("sample", n)
    spark.sql(
        f"""CREATE OR REPLACE TEMP VIEW todo AS
            SELECT s.input_hash, s.text FROM sample s
            LEFT ANTI JOIN {LABELS} l
              ON l.input_hash = s.input_hash AND l.model = '{model}'
             AND l.prompt_version = '{PROMPT_VERSION}'"""
    )
    todo = spark.table("todo").count()
    batch, calls, seconds = call_model("todo", model, 0.0, "enrich")
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
    errors = spark.table(CALLS).where((F.col("batch_id") == batch) & F.col("error").isNotNull()).count()
    return {"todo": todo, "batch": batch, "calls": calls, "errors": errors, "seconds": seconds}


# COMMAND ----------

# MAGIC %md
# MAGIC ## A — load the reviews; one row per distinct text

# COMMAND ----------

raw = (
    spark.read.option("header", True).option("multiLine", True).option("escape", '"')
    .csv(SRC_CSV)
)
(
    raw.select(
        "review_id", "order_id",
        F.col("review_score").cast("int").alias("review_score"),
        F.trim("review_comment_message").alias("text"),
    )
    .withColumn("input_hash", F.when(F.col("text") != "", F.sha2("text", 256)))
    .write.saveAsTable(REVIEWS)
)
spark.sql(
    f"""CREATE TABLE {TEXTS} AS
        SELECT input_hash, any_value(text) AS text, count(*) AS records
        FROM {REVIEWS} WHERE input_hash IS NOT NULL GROUP BY input_hash"""
)
spark.sql(
    f"""CREATE TABLE {CALLS} (batch_id INT, purpose STRING, input_hash STRING, model STRING,
        prompt_version STRING, temperature DOUBLE, response STRING, error STRING, called_at TIMESTAMP)"""
)
spark.sql(
    f"""CREATE TABLE {LABELS} (input_hash STRING NOT NULL, model STRING NOT NULL,
        prompt_version STRING NOT NULL, sentiment STRING, topic STRING, label_ok BOOLEAN,
        response STRING, called_at TIMESTAMP, batch_id INT)"""
)
records = spark.table(REVIEWS).count()
with_text = spark.table(REVIEWS).where("input_hash IS NOT NULL").count()
distinct = spark.table(TEXTS).count()
saved = with_text - distinct
print(f"A: {records:,} records, {with_text:,} with text, {distinct:,} distinct texts "
      f"-> {saved:,} calls saved by keying on the text ({saved / with_text:.1%})")
print("   predicted 99,224 / 40,950 / 35,616 (pandas, locally)")
assert records == 99_224, "the CSV must parse to one row per record (multiLine + escape)"

# COMMAND ----------

# MAGIC %md
# MAGIC ## B — run 1: 1,000 texts, 70B, temperature 0

# COMMAND ----------

b = enrich(N1, BIG)
b_labels = spark.table(LABELS)
b_bad = b_labels.where("NOT label_ok").count()
print(f"B: {b}")
print(f"   label rows {b_labels.count():,}; bad answers {b_bad} ({b_bad / max(b_labels.count(), 1):.1%})")
for r in b_labels.where("NOT label_ok").select("response").limit(10).collect():
    print(f"   bad: {r.response!r}")
print(f"   MERGE metrics: {dict(merges(LABELS)[-1].operationMetrics)}")
assert b["todo"] == N1 and b["calls"] == N1
assert b_labels.count() == N1 - b["errors"], "every call without an error is labelled, once"

# COMMAND ----------

# MAGIC %md
# MAGIC ## C — run 1 again, nothing changed: the model must not be asked

# COMMAND ----------

before = fingerprint(LABELS)
merges_before = len(merges(LABELS))
c = enrich(N1, BIG)
after = fingerprint(LABELS)
c_merges = merges(LABELS)
print(f"C: {c}")
print(f"   labels {before} -> {after}; MERGE commits {merges_before} -> {len(c_merges)}"
      + (f", last metrics {dict(c_merges[-1].operationMetrics)}" if len(c_merges) > merges_before else ""))
if b["errors"] == 0:
    assert c["calls"] == 0, "a re-run asks the model nothing"
    assert after == before, "a re-run changes no label"
else:
    print(f"   run 1 had {b['errors']} transient errors: C retried exactly those ({c['calls']} calls)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D — the sample grows to 1,500: only the new 500 are asked

# COMMAND ----------

first_keys = f"input_hash IN (SELECT input_hash FROM {TEXTS} ORDER BY input_hash LIMIT {N1})"
old_before = fingerprint(LABELS, f"{first_keys} AND model = '{BIG}'")
d = enrich(N2, BIG)
old_after = fingerprint(LABELS, f"{first_keys} AND model = '{BIG}'")
print(f"D: {d}; first 1,000: {old_before} -> {old_after}")
assert d["todo"] == N2 - N1 + c["errors"] and old_after == old_before

# COMMAND ----------

# MAGIC %md
# MAGIC ## E — repeatability: the same 200 prompts, 5 times, at temperature 0 and at 1.0
# MAGIC Each repeat is its own query: inside one query, identical `ai_query` expressions might be computed
# MAGIC once and reused, which would fake a perfect score.

# COMMAND ----------

sample_view("repeat_sample", N_REPEAT)
e_batches = {}
for temperature in (0.0, 1.0):
    for i in range(REPEATS):
        batch, calls, seconds = call_model("repeat_sample", BIG, temperature, f"repeat_t{temperature}")
        e_batches.setdefault(temperature, []).append(batch)
        print(f"E: t={temperature} run {i + 1}: batch {batch}, {calls} calls, {seconds}s")

for temperature, batches in e_batches.items():
    per_text = spark.sql(
        f"""SELECT input_hash,
                   count(DISTINCT regexp_replace(lower(response), '\\\\s', '')) AS answers,
                   count_if(error IS NOT NULL) AS errors
            FROM {CALLS} WHERE batch_id IN ({','.join(map(str, batches))}) GROUP BY input_hash"""
    )
    changed = per_text.where("answers > 1").count()
    errors = per_text.agg(F.sum("errors")).first()[0]
    print(f"E: t={temperature}: {changed} of {N_REPEAT} texts got more than one answer in {REPEATS} asks "
          f"({changed / N_REPEAT:.1%}); errors {errors}")
    vs_b = spark.sql(
        f"""SELECT count_if(regexp_replace(lower(c.response), '\\\\s', '')
                            <> l.sentiment || ',' || l.topic) AS differ
            FROM {CALLS} c JOIN {LABELS} l ON l.input_hash = c.input_hash AND l.model = '{BIG}'
            WHERE c.batch_id = {batches[0]} AND l.label_ok"""
    ).first().differ
    print(f"   t={temperature}: first repeat vs the stored label (run 1): {vs_b} of {N_REPEAT} differ")
    for r in (
        spark.sql(
            f"""SELECT t.text, collect_set(regexp_replace(lower(c.response), '\\\\s', '')) AS answers
                FROM {CALLS} c JOIN {TEXTS} t USING (input_hash)
                WHERE c.batch_id IN ({','.join(map(str, batches))})
                GROUP BY t.text HAVING size(answers) > 1 LIMIT 5"""
        ).collect()
    ):
        print(f"   {r.answers} <- {r.text[:90]!r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## F — model change: a new key, a deliberate backfill; the old answers stay

# COMMAND ----------

spark.sql(
    f"""CREATE OR REPLACE VIEW {CURRENT} AS
        SELECT input_hash, sentiment, topic, model, prompt_version FROM {LABELS}
        WHERE model = '{BIG}' AND prompt_version = '{PROMPT_VERSION}' AND label_ok"""
)
big_before = fingerprint(LABELS, f"model = '{BIG}'")
current_before = fingerprint(CURRENT)
f = enrich(N2, SMALL)
print(f"F: {f}")
print(f"   70B rows {big_before} -> {fingerprint(LABELS, f'model = {BIG!r}')}; "
      f"pinned view {current_before} -> {fingerprint(CURRENT)}")
small_bad = spark.table(LABELS).where(f"model = '{SMALL}' AND NOT label_ok").count()
agree = spark.sql(
    f"""SELECT count(*) AS both_ok,
               count_if(a.sentiment = b.sentiment) AS same_sentiment,
               count_if(a.topic = b.topic) AS same_topic,
               count_if(a.sentiment = b.sentiment AND a.topic = b.topic) AS same_both
        FROM {LABELS} a JOIN {LABELS} b USING (input_hash, prompt_version)
        WHERE a.model = '{BIG}' AND b.model = '{SMALL}' AND a.label_ok AND b.label_ok"""
).first()
print(f"   8B bad answers {small_bad}; on {agree.both_ok:,} texts both answered: sentiment "
      f"{agree.same_sentiment / agree.both_ok:.1%}, topic {agree.same_topic / agree.both_ok:.1%}, "
      f"both {agree.same_both / agree.both_ok:.1%} the same")
assert fingerprint(LABELS, f"model = '{BIG}'") == big_before and fingerprint(CURRENT) == current_before

# COMMAND ----------

# MAGIC %md
# MAGIC ## G — graded: sentiment against the customer's own stars

# COMMAND ----------

graded = spark.sql(
    f"""SELECT l.model, r.review_score, l.sentiment,
               CASE WHEN r.review_score <= 2 THEN 'negative' WHEN r.review_score = 3 THEN 'neutral'
                    ELSE 'positive' END AS from_stars
        FROM {REVIEWS} r JOIN {LABELS} l USING (input_hash)
        WHERE l.label_ok AND l.prompt_version = '{PROMPT_VERSION}'"""
)
for r in (
    graded.groupBy("model")
    .agg(F.count("*").alias("records"), F.count_if(F.col("sentiment") == F.col("from_stars")).alias("agree"),
         F.count_if((F.col("sentiment") == "negative") & (F.col("review_score") >= 4)).alias("neg_with_4_5"),
         F.count_if((F.col("sentiment") == "positive") & (F.col("review_score") <= 2)).alias("pos_with_1_2"))
    .orderBy("model").collect()
):
    print(f"G: {r.model}: {r.agree:,} of {r.records:,} records agree with the stars "
          f"({r.agree / r.records:.1%}); "
          f"negative text with 4-5 stars {r.neg_with_4_5}, positive text with 1-2 stars {r.pos_with_1_2}")
print("   per star (70B):")
for r in (
    graded.where(F.col("model") == BIG).groupBy("review_score", "sentiment").count()
    .orderBy("review_score", "sentiment").collect()
):
    print(f"   {r.review_score} stars -> {r.sentiment}: {r['count']:,}")
print("   disagreements to read (70B):")
for r in spark.sql(
    f"""SELECT r.review_score, l.sentiment, l.topic, r.text
        FROM {REVIEWS} r JOIN {LABELS} l USING (input_hash)
        WHERE l.model = '{BIG}' AND l.label_ok
          AND ((r.review_score >= 4 AND l.sentiment = 'negative')
            OR (r.review_score <= 2 AND l.sentiment = 'positive'))
        ORDER BY r.review_id LIMIT 8"""
).collect():
    print(f"   {r.review_score}* {r.sentiment}/{r.topic}: {r.text[:110]!r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## H — `ai_classify` against `ai_query`'s topic, on the 200 texts

# COMMAND ----------

started = time.time()
spark.sql(
    f"""CREATE TABLE {CLASSIFY} AS
        SELECT input_hash, ai_classify(text, ARRAY({', '.join(repr(t) for t in TOPICS)})) AS topic
        FROM repeat_sample"""
)
h_seconds = round(time.time() - started, 1)
h = spark.sql(
    f"""SELECT count(*) AS n, count_if(c.topic = l.topic) AS same, count_if(c.topic IS NULL) AS no_answer
        FROM {CLASSIFY} c JOIN {LABELS} l USING (input_hash)
        WHERE l.model = '{BIG}' AND l.label_ok"""
).first()
print(f"H: ai_classify {h_seconds}s; same topic as ai_query (70B) on {h.same} of {h.n} ({h.same / h.n:.1%}); "
      f"no answer {h.no_answer}")
for r in spark.sql(
    f"""SELECT c.topic AS classify, l.topic AS query, count(*) AS n FROM {CLASSIFY} c
        JOIN {LABELS} l USING (input_hash) WHERE l.model = '{BIG}' AND l.label_ok AND c.topic <> l.topic
        GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 6"""
).collect():
    print(f"   ai_classify {r.classify} vs ai_query {r.query}: {r.n}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — the numbers for the "where AI falls short" table

# COMMAND ----------

total = spark.table(CALLS).groupBy("purpose").agg(
    F.count("*").alias("calls"), F.count_if(F.col("error").isNotNull()).alias("errors")
).orderBy("purpose").collect()
print(f"A distinct texts {distinct:,} of {with_text:,} with text")
print(f"B run 1: {b['calls']} calls, {b['errors']} errors, {b_bad} bad answers, {b['seconds']}s")
print(f"C re-run: {c['calls']} calls, labels unchanged {after == before}")
print(f"D grow:   {d['calls']} calls ({d['seconds']}s), first 1,000 unchanged {old_after == old_before}")
print(f"F 8B:     {f['calls']} calls, {small_bad} bad; "
      f"agreement sentiment {agree.same_sentiment}/{agree.both_ok}, "
      f"topic {agree.same_topic}/{agree.both_ok}")
print(f"H ai_classify: {h.same}/{h.n} same topic")
for r in total:
    print(f"calls by purpose: {r.purpose}: {r.calls:,} ({r.errors} errors)")
