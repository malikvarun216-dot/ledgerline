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
# MAGIC | G. broken file | a duplicate key; then 2% bad zips; then fixed | refused whole twice; then applied |
# MAGIC | H. one bad row | one bad zip; then re-delivered fixed | night applied, seller held; then fixed |
# MAGIC
# MAGIC Parts A and B are the deliberate failure: the bug is asserted **present** first. C to H test the
# MAGIC decisions that follow from the fix (decisions.md, Sessions 8 and 9). Uses the production code
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
# MAGIC ## G. A file that looks broken is refused whole — and applied once it is corrected
# MAGIC
# MAGIC The production function `apply_dim`, pointed at two scratch schemas with their own merge log.
# MAGIC Night 2017-08-30 is clean. Night 2017-12-28 is delivered broken twice, then corrected:
# MAGIC - **G1** one seller delivered twice — which of the two rows is true cannot be decided row by row;
# MAGIC - **G2** 60 of 2,995 zips not numbers (2.0%, over the 1% line) — the file is broken, not a row.
# MAGIC
# MAGIC Expected each time: refused before its MERGE, the reason named, Silver still at the clean night,
# MAGIC nothing logged. Then the corrected night arrives and the next run applies it.
# MAGIC
# MAGIC Session 8's G planted one bad zip, one bad date and a duplicate, and expected all three to refuse
# MAGIC the night. Under Session 9's rule (decisions.md, revision of 2026-10-01) the two bad values would be
# MAGIC **held** instead (part H); only the duplicate still refuses.

# COMMAND ----------

G_BRONZE, G_SILVER = "workspace.exp03_bronze", "workspace.exp03_silver"
G_LOG, G_REJECTED = f"{G_SILVER}.dims_merge_log", f"{G_SILVER}.dims_rejected_rows"
for schema in (G_BRONZE, G_SILVER):
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
create_merge_log(G_LOG)

real = spark.table(f"{BRONZE}.{DIM}").where(F.col("dump_date").isin(OLD, NEW))
new_keys = sorted(keys(new))


def planted(on_keys, column, value):
    on_new = (F.col("dump_date") == NEW) & F.col(KEY).isin(on_keys)
    return F.when(on_new, F.lit(value)).otherwise(F.col(column))


def deliver_new(rows, bronze, minutes_later=0):
    """Replace night NEW in a scratch Bronze, as Auto Loader does for a re-delivered file."""
    rows = rows.where(F.col("dump_date") == NEW).withColumn(
        "_source_modified_at", F.col("_source_modified_at") + F.expr(f"INTERVAL {minutes_later} MINUTES")
    )
    (
        rows.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"dump_date = '{NEW}'").saveAsTable(f"{bronze}.{DIM}")
    )


def refused_by(bronze, silver, log):
    try:
        apply_dim(DIM, bronze=bronze, silver=silver, log=log, rejected=f"{silver}.dims_rejected_rows")
        return None
    except NightRefused as e:
        return str(e)


g_table = f"{G_SILVER}.{DIM}"
twice = real.where((F.col("dump_date") == NEW) & (F.col(KEY) == new_keys[0]))
real.unionByName(twice).write.format("delta").saveAsTable(f"{G_BRONZE}.{DIM}")

g1 = refused_by(G_BRONZE, G_SILVER, G_LOG)
print(f"G1 (a seller twice): {g1}")
assert g1 is not None and "duplicate_keys" in g1

ZIP = "seller_zip_code_prefix"
sixty_bad = real.withColumn(ZIP, planted(new_keys[:60], ZIP, "ABCDE"))
deliver_new(sixty_bad, G_BRONZE)
g2 = refused_by(G_BRONZE, G_SILVER, G_LOG)
print(f"G2 (60 bad zips, 2.0%): {g2}")
assert g2 is not None and "bad_rows" in g2 and "bad__seller_zip_code_prefix" in g2

assert [str(r.dump_date) for r in spark.table(G_LOG).collect()] == [OLD], "only the clean night logged"
assert spark.sql(f"DESCRIBE HISTORY {g_table}").where("operation = 'MERGE'").count() == 1, "a bad MERGE ran"
assert compare_to_night(g_table, old, DIM) == (0, 0), "Silver still shows the clean night"

deliver_new(real, G_BRONZE)
apply_dim(DIM, bronze=G_BRONZE, silver=G_SILVER, log=G_LOG, rejected=G_REJECTED)
assert [str(r.dump_date) for r in spark.table(G_LOG).orderBy("dump_date").collect()] == [OLD, NEW]
assert compare_to_night(g_table, new, DIM) == (0, 0), "the corrected night is applied in full"
print("GUARD SEEN: both broken files were refused before their MERGE, Silver kept the clean night, "
      "and the corrected night was applied by the next run")

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. One bad row is held: the night is applied, that seller is not deleted and keeps its old values
# MAGIC
# MAGIC One seller whose values really change on the new night gets a zip of `ABCDE`. Expected: the night is
# MAGIC applied (49 updates, 100 deletes); the bad seller is **still in Silver with its previous values**
# MAGIC (`_ok = false`: its key is in the source, so no delete; it may not update); one row in the
# MAGIC rejected-rows table; `held = 1` in the merge log. Under Session 8's rule the same file would have
# MAGIC been refused whole: 50 updates and 100 deletes held back for one zip. Under part D's "set it aside",
# MAGIC the seller would have been deleted.
# MAGIC
# MAGIC Then the source corrects the file and re-delivers the same night (a newer file time). The next run
# MAGIC re-applies that night — Session 8's correction path, written then and driven for the first time
# MAGIC here: one update, nothing else, nothing held.

# COMMAND ----------

H_BRONZE, H_SILVER = "workspace.exp03h_bronze", "workspace.exp03h_silver"
H_LOG, H_REJECTED = f"{H_SILVER}.dims_merge_log", f"{H_SILVER}.dims_rejected_rows"
for schema in (H_BRONZE, H_SILVER):
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
create_merge_log(H_LOG)
h_table = f"{H_SILVER}.{DIM}"

cols = business_columns(DIM)
differs = new.select(*cols).join(old.select(*cols), cols, "left_anti")  # new or changed
changed = sorted(keys(differs.join(old.select(KEY), KEY)))  # ...and already there before
victim = changed[0]
print(f"{len(changed)} sellers change on {NEW}; the bad zip goes to {victim}")
assert len(changed) == 50

one_bad = real.withColumn(ZIP, planted([victim], ZIP, "ABCDE"))
one_bad.write.format("delta").saveAsTable(f"{H_BRONZE}.{DIM}")
held = apply_dim(DIM, bronze=H_BRONZE, silver=H_SILVER, log=H_LOG, rejected=H_REJECTED)

line = spark.table(H_LOG).where(F.col("dump_date") == NEW).first()
kept = spark.table(h_table).where(F.col(KEY) == victim).select(*cols, "_last_changed_dump_date").first()
was = old.where(F.col(KEY) == victim).select(*cols).first()
rejected = spark.table(H_REJECTED).collect()
print(f"H (one bad row): log +{line.inserted} ~{line.updated} -{line.deleted} held {line.held}")
print(f"   {victim} in Silver: {kept}\n   rejected rows: {[r.asDict() for r in rejected]}")

assert held == 1 and line.held == 1
assert (line.inserted, line.updated, line.deleted) == (0, 49, 100)
assert kept is not None, "the held seller was deleted — the filter trap"
assert [kept[c] for c in cols] == [was[c] for c in cols], "the held seller must keep its previous values"
assert str(kept._last_changed_dump_date) == OLD
assert [(r.key, r.column, r.raw_value) for r in rejected] == [(victim, ZIP, "ABCDE")]
bad_new = typed_night(DIM, bronze_night(DIM, NEW, H_BRONZE))
assert compare_to_night(h_table, bad_new, DIM) == (0, 0), "every other seller is at the new night"

# The source corrects the file and re-delivers it: same night, newer file time.
deliver_new(real, H_BRONZE, minutes_later=60)
apply_dim(DIM, bronze=H_BRONZE, silver=H_SILVER, log=H_LOG, rejected=H_REJECTED)
again = spark.table(H_LOG).where(F.col("dump_date") == NEW).orderBy(F.col("applied_at").desc()).first()
fixed = spark.table(h_table).where(F.col(KEY) == victim).first()
print(f"   corrected night re-applied: +{again.inserted} ~{again.updated} -{again.deleted} held {again.held}")

assert (again.inserted, again.updated, again.deleted, again.held) == (0, 1, 0, 0)
assert spark.table(H_REJECTED).count() == 0, "the corrected night clears its held rows"
assert str(fixed._last_changed_dump_date) == NEW
assert compare_to_night(h_table, new, DIM) == (0, 0), "Silver now equals the corrected night"
print("FIX SEEN: one bad row held the seller at its old values, the rest of the night applied, nothing "
      "deleted by mistake; the corrected re-delivery applied exactly that one row")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look — the bug table vs the fix table

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {SILVER}.exp03_bug").select("version", "operation", "operationMetrics"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {SILVER}.exp03_fix").select("version", "operation", "operationMetrics"))