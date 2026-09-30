# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # exp_02 — partition overwrite: deletions for free, and the overwrite that deletes too much
# MAGIC
# MAGIC **The pattern.** Bronze dims hold every night side by side. When a night is delivered again —
# MAGIC corrected, or replayed — Bronze must end up with *that night's new rows, and nothing else
# MAGIC touched*. Production does it with `replaceWhere dump_date IN (<nights in this batch>)`:
# MAGIC "delete this night's rows, write the new ones", in one commit. A row the corrected file no
# MAGIC longer has is simply not rewritten — **a deletion handled for free**, the opposite of the
# MAGIC plain MERGE in `exp_03`, which kept every deleted row.
# MAGIC
# MAGIC **Real data.** Seller nights 2017-08-30 (3,095) and 2017-12-28 (2,995) from Bronze, read-only.
# MAGIC The re-delivery is night 2017-12-28 again, with **one seller removed** (2,994 rows).
# MAGIC
# MAGIC | Part | How the night is written again | Expected |
# MAGIC |---|---|---|
# MAGIC | A. append | `mode("append")` | the night **doubles** (5,989); the removed seller stays |
# MAGIC | B. overwrite | `mode("overwrite")`, no predicate | the **whole table** is that one night |
# MAGIC | C. `replaceWhere` | production `write_batch` | that night = 2,994 new rows, other untouched |
# MAGIC | D. mislabelled file | folder 12-28, rows 08-30 | refused; check off: the **wrong night** goes |
# MAGIC | E. stray row | replace 12-28, one row dated 08-30 | Delta itself refuses the write |
# MAGIC
# MAGIC A and B are the bugs, asserted **present** first; C is the fix. D and E fire two guards that
# MAGIC Session 6 wrote about and nobody had seen fire. Scratch tables only (`workspace.bronze.exp02_*`),
# MAGIC rebuilt every run. Scheduled late: this should have run in Session 6 (incidents.md 2026-09-30).

# COMMAND ----------

# MAGIC %run ../databricks/bronze/_autoload_dims

# COMMAND ----------

SOURCE = "workspace.bronze.seller"
OLD, NEW = "2017-08-30", "2017-12-28"
BUSINESS = [
    "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state", "dim_updated_at", "dump_date",
]


def batch(night):
    """One night as the Auto Loader stream hands it to write_batch: the rows plus the folder's date."""
    rows = spark.table(SOURCE).where(F.col("dump_date") == night)
    return rows.withColumn("_path_dump_date", F.col("dump_date"))


def fresh(name):
    """A scratch Bronze table holding both nights, written the production way, one batch each."""
    table = f"workspace.bronze.exp02_{name}"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    batch(OLD).drop("_path_dump_date").limit(0).write.format("delta").saveAsTable(table)
    write_batch(batch(OLD), 0, table)
    write_batch(batch(NEW), 1, table)
    return table


def rows_of(table, night):
    return spark.table(table).where(F.col("dump_date") == night).select(*BUSINESS)


def same(a, b):
    return a.exceptAll(b).count() == 0 and b.exceptAll(a).count() == 0


def has(table, seller):
    return spark.table(table).where((F.col("dump_date") == NEW) & (F.col("seller_id") == seller)).count()


VICTIM = batch(NEW).agg(F.min("seller_id")).first()[0]
redelivery = batch(NEW).where(F.col("seller_id") != VICTIM)  # the corrected file: one seller retracted
original_old = rows_of(SOURCE, OLD)
print(f"{OLD}: {original_old.count():,}   {NEW}: {batch(NEW).count():,}   re-delivery of {NEW}: "
      f"{redelivery.count():,} (seller {VICTIM} removed)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Append — the re-delivered night lands beside the first copy

# COMMAND ----------

appended = fresh("append")
redelivery.drop("_path_dump_date").write.format("delta").mode("append").saveAsTable(appended)
a = night_counts(appended)
twice = spark.table(appended).where(F.col("dump_date") == NEW).groupBy("seller_id").count()
dupes = twice.where("count > 1").count()
print(f"A (append): {dict(sorted(a.items()))}  sellers twice in {NEW}: {dupes:,}  "
      f"removed seller still there: {has(appended, VICTIM)}")
assert a == {OLD: 3_095, NEW: 2_995 + 2_994}, "the night doubled"
assert dupes == 2_994 and has(appended, VICTIM) == 1
print("BUG PRESENT: the night doubled, and the seller the source removed is still in Bronze — no error")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Overwrite with no predicate — one night replaces the whole table

# COMMAND ----------

overwritten = fresh("overwrite")
redelivery.drop("_path_dump_date").write.format("delta").mode("overwrite").saveAsTable(overwritten)
b = night_counts(overwritten)
print(f"B (overwrite, no predicate): {dict(sorted(b.items()))}")
assert b == {NEW: 2_994}, "every other night is gone"
print(f"BUG PRESENT: night {OLD} ({original_old.count():,} rows) deleted by a write meant to replace {NEW}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. `replaceWhere` — the production write: only that night changes, and the deletion comes free

# COMMAND ----------

fixed = fresh("replacewhere")
write_batch(redelivery, 2, fixed)
c = night_counts(fixed)
last = replace_commits(fixed)[-1]
print(f"C (replaceWhere): {dict(sorted(c.items()))}  removed seller still there: {has(fixed, VICTIM)}")
print(f"   last commit: version {last[0]}, predicate {last[1]}, rows written {last[2]}")
assert c == {OLD: 3_095, NEW: 2_994}
assert has(fixed, VICTIM) == 0, "the deletion came free: the new night simply does not contain it"
assert same(rows_of(fixed, OLD), original_old), "the other night is untouched, every column"
assert same(rows_of(fixed, NEW), redelivery.select(*BUSINESS)), "the night is exactly the corrected file"
print("FIXED: one night replaced, the retracted seller gone, the other night identical")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. A mislabelled file — the folder says one night, the rows say another
# MAGIC
# MAGIC `replaceWhere` replaces the nights **named by the rows**. A file in folder `dump_date=2017-12-28`
# MAGIC whose rows say `2017-08-30` would make it replace the wrong night. `write_batch` checks the
# MAGIC column against the folder first (Session 6) — never seen firing until now.

# COMMAND ----------

mislabelled = batch(NEW).withColumn("dump_date", F.lit(OLD))  # _path_dump_date still says NEW
guarded = fresh("mislabelled")
version_before = replace_commits(guarded)[-1][0]
try:
    write_batch(mislabelled, 3, guarded)
    refused = None
except ValueError as e:
    refused = str(e)
print(f"D (guard on): {refused}")
assert refused and "disagrees with its folder" in refused
assert replace_commits(guarded)[-1][0] == version_before, "nothing written"

# Without the check (scratch, on purpose): the predicate comes from the rows, so night OLD is replaced.
(
    mislabelled.drop("_path_dump_date").write.format("delta").mode("overwrite")
    .option("replaceWhere", f"dump_date IN ('{OLD}')").saveAsTable(guarded)
)
d = night_counts(guarded)
print(f"   guard off: {dict(sorted(d.items()))} — night {OLD} now holds {NEW}'s sellers")
assert not same(rows_of(guarded, OLD), original_old), "the real night-OLD rows are gone"
gone_for_good = original_old.select("seller_id").subtract(spark.table(guarded).select("seller_id")).count()
print(f"   sellers that existed only on {OLD} and now exist nowhere in the table: {gone_for_good}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. A stray row — Delta checks every written row against the predicate
# MAGIC
# MAGIC Session 6's decision says Delta "checks every written row matches" `replaceWhere`, so a row with
# MAGIC a stray date fails the write instead of landing. Claimed, never seen. One row dated OLD inside a
# MAGIC write that replaces NEW:

# COMMAND ----------

strict = fresh("stray")
version_before = replace_commits(strict)[-1][0]
stray = batch(NEW).drop("_path_dump_date").withColumn(
    "dump_date", F.when(F.col("seller_id") == VICTIM, F.lit(OLD)).otherwise(F.col("dump_date"))
)
try:
    (
        stray.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"dump_date IN ('{NEW}')").saveAsTable(strict)
    )
    rejected = None
except Exception as e:  # the class differs across runtimes; the message is what is checked
    rejected = f"{type(e).__name__}: {str(e)[:300]}"
print(f"E (stray row): {rejected}")
assert rejected is not None, "Delta let a row outside the predicate through"
assert replace_commits(strict)[-1][0] == version_before, "nothing written"
assert night_counts(strict) == {OLD: 3_095, NEW: 2_995}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look — three writes, three different commits

# COMMAND ----------

for name in ("append", "overwrite", "replacewhere"):
    print(name)
    display(
        spark.sql(f"DESCRIBE HISTORY workspace.bronze.exp02_{name}")
        .select("version", "operation", "operationParameters", "operationMetrics")
    )