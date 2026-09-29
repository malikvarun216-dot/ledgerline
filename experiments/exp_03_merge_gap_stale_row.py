# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # exp_03 — the MERGE gap: a row deleted at the source survives a plain MERGE
# MAGIC
# MAGIC **The gap.** A plain MERGE has two clauses: *matched → update*, *not matched → insert*. Both are
# MAGIC about rows that are **in** tonight's file. A seller who is **no longer in the file** matches
# MAGIC nothing, so neither clause touches it: it stays in Silver forever, with its old values, and no
# MAGIC error is raised. Only `WHEN NOT MATCHED BY SOURCE THEN DELETE` looks at Silver rows the file lacks.
# MAGIC
# MAGIC **Real data.** Seller nights 2017-08-30 (3,095 sellers) and 2017-12-28 (2,995) from Bronze,
# MAGIC read-only. The generator reports for the second night: **50 changed, 100 gone, 0 new**.
# MAGIC
# MAGIC | Part | What runs | Expected |
# MAGIC |---|---|---|
# MAGIC | A. bug | plain MERGE, no delete clause | 100 gone sellers **still in Silver**; no error |
# MAGIC | B. fix | + `WHEN NOT MATCHED BY SOURCE DELETE` | Silver equals the new night exactly |
# MAGIC | C. again | B's MERGE a second time | 0 / 0 / 0: a full snapshot applied twice changes nothing |
# MAGIC | D. filter trap | one seller "set aside as bad" before the MERGE | that seller **deleted** |
# MAGIC | E. truncated file | half of the new night | refused; with the breaker off, ~half deleted |
# MAGIC | F. no change test | `WHEN MATCHED THEN UPDATE` for every match | 2,995 updates instead of 50 |
# MAGIC
# MAGIC Parts A and B are the deliberate failure: the bug is asserted **present** first. C to F test the
# MAGIC decisions that follow from the fix (decisions.md, Session 8). Uses the production code
# MAGIC (`databricks/silver/_merge_dims`); the only differences are the switches each part names.
# MAGIC **Scratch tables only** (`workspace.silver.exp03_*`), rebuilt every run.

# COMMAND ----------

# MAGIC %run ../databricks/silver/_merge_dims

# COMMAND ----------

DIM = "seller"
KEY = DIMS[DIM]["key"]
OLD, NEW = "2017-08-30", "2017-12-28"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")


def fresh(name):
    """A new, empty scratch Silver table with the production columns and properties."""
    table = f"{SILVER}.exp03_{name}"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    create_silver(table, DIM)
    return table


def night(n):
    raw = bronze_night(DIM, n)
    _, problems = check_night(DIM, raw)
    assert not problems, f"night {n} is not a clean file: {problems}"
    return typed_night(DIM, raw)


def keys(df):
    return {r[0] for r in df.select(KEY).collect()}


old, new = night(OLD), night(NEW)
GONE = keys(old) - keys(new)
print(f"{OLD}: {old.count():,} sellers   {NEW}: {new.count():,} sellers   gone between them: {len(GONE)}")
assert len(GONE) == 100, "the generator removed 100 sellers on the new night"

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. The bug — a plain MERGE keeps every deleted seller

# COMMAND ----------

bug = fresh("bug")
merge_night(bug, old, DIM)
a = merge_night(bug, new, DIM, delete_missing=False)
stale = keys(spark.table(bug)) - keys(new)
print(f"A (plain MERGE): {a}  rows in Silver {spark.table(bug).count():,}  stale sellers {len(stale)}")

# The bug, present: nothing deleted, no error, and exactly the gone sellers left behind ...
assert (a["inserted"], a["updated"], a["deleted"]) == (0, 50, 0)
assert stale == GONE, "the stale rows are exactly the sellers the source deleted"
# ... still holding their last values from the old night, as if nothing had happened.
cols = business_columns(DIM)
left_behind = spark.table(bug).where(F.col(KEY).isin(list(GONE))).select(*cols)
assert left_behind.exceptAll(old.where(F.col(KEY).isin(list(GONE))).select(*cols)).count() == 0
print("BUG PRESENT: 100 sellers deleted at the source are still in Silver, and the MERGE reported success")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. The fix — `WHEN NOT MATCHED BY SOURCE THEN DELETE`

# COMMAND ----------

fix = fresh("fix")
merge_night(fix, old, DIM)
b = merge_night(fix, new, DIM)
extra, missing = compare_to_night(fix, new, DIM)
print(f"B (with the delete clause): {b}  rows {spark.table(fix).count():,}  extra={extra} missing={missing}")
assert (b["inserted"], b["updated"], b["deleted"]) == (0, 50, 100)
assert (extra, missing) == (0, 0), "Silver equals the new night, every column of every row"
assert not keys(spark.table(fix)) & GONE
print("FIXED: the 100 gone sellers are deleted; Silver equals the new night exactly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Apply the same night again — nothing changes
# MAGIC
# MAGIC This is why the merge log need not be written in the same commit as the MERGE: if a run dies
# MAGIC between the two, the night is applied a second time, and a full snapshot applied twice lands
# MAGIC on the same table. Also recorded: whether Delta writes a commit for a MERGE that changes nothing.

# COMMAND ----------

version_before = latest_version(fix)
c = merge_night(fix, new, DIM)
print(f"C (same night again): {c}  table version {version_before} -> {latest_version(fix)}")
assert (c["inserted"], c["updated"], c["deleted"]) == (0, 0, 0)
assert compare_to_night(fix, new, DIM) == (0, 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. The filter trap — setting one row aside deletes it
# MAGIC
# MAGIC Kafka Bronze sends a bad message to a quarantine table (Session 7). Do the same here — drop one
# MAGIC "bad" seller from the source before the MERGE — and `NOT MATCHED BY SOURCE` reads its absence
# MAGIC as a deletion. Then re-apply the full night: the seller comes back, but as a **new** row.

# COMMAND ----------

victim = sorted(keys(new))[0]
first_seen = spark.table(fix).where(F.col(KEY) == victim).first()._first_seen_dump_date
d = merge_night(fix, new.where(F.col(KEY) != victim), DIM)
gone_now = spark.table(fix).where(F.col(KEY) == victim).count() == 0
print(f"D (one row set aside): {d}  seller {victim} deleted from Silver: {gone_now}")
assert d["deleted"] == 1 and gone_now

back = merge_night(fix, new, DIM)
first_seen_after = spark.table(fix).where(F.col(KEY) == victim).first()._first_seen_dump_date
print(f"   full night re-applied: {back}  first seen {first_seen} -> {first_seen_after}")
assert back["inserted"] == 1
assert first_seen_after != first_seen, "re-inserting does not restore history; only time travel would"
print("TRAP SHOWN: a filtered source row became a delete; re-running brought the row back, not its history")

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. A truncated file — the breaker refuses; without it, half of Silver goes

# COMMAND ----------

truncated_table = fresh("truncated")
merge_night(truncated_table, old, DIM)
half = new.where(F.abs(F.hash(KEY)) % 2 == 0)  # a "file" cut off halfway, deterministic
version_before = latest_version(truncated_table)
try:
    check_deletes(truncated_table, half, DIM)
    refused = None
except MassDeleteRefused as e:
    refused = str(e)
print(f"E (breaker on): {refused}")
assert refused is not None, "the breaker let a half-empty file through"
assert latest_version(truncated_table) == version_before, "nothing may be written when refused"

# What a green run would have done without the breaker (scratch table, on purpose):
current, doomed = check_deletes(truncated_table, half, DIM, allowed=True)
e = merge_night(truncated_table, half, DIM)
print(f"   breaker off: {e}  {doomed:,} of {current:,} sellers deleted ({doomed / current:.0%}), no error")
assert e["deleted"] == doomed and doomed / current > MAX_DELETE_FRACTION

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. No change test — every matched row rewritten, every night

# COMMAND ----------

everything = fresh("all")
merge_night(everything, old, DIM)
f = merge_night(everything, new, DIM, only_changed=False)
feed_all = change_counts(everything).get(f["version"], {})
feed_fix = change_counts(fix).get(b["version"], {})
print(f"F (update every match): {f}  feed update_postimage {feed_all.get('update_postimage', 0):,}")
print(f"   with the change test (B): {b}  feed update_postimage {feed_fix.get('update_postimage', 0):,}")
assert f["updated"] == new.count(), "every seller still in the file was 'updated'"
assert feed_fix.get("update_postimage", 0) == 50
assert compare_to_night(everything, new, DIM) == (0, 0), "same final table — the cost is invisible there"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look — the bug table vs the fix table

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {SILVER}.exp03_bug").select("version", "operation", "operationMetrics"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {SILVER}.exp03_fix").select("version", "operation", "operationMetrics"))