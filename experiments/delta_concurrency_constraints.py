# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Two writers at once, and rules the engine enforces — Delta, seen rather than read
# MAGIC
# MAGIC Two topics `docs/coverage.md` binds to Session 10, on scratch copies of Silver
# MAGIC (`workspace.silver.dx_*`, rebuilt every run; the real tables are only read).
# MAGIC
# MAGIC **Part 1 — concurrent writes.** Delta has no locks. Each writer reads a version, does its work, and at
# MAGIC commit checks whether anything committed since conflicts with what it read and wrote (*optimistic
# MAGIC concurrency*). If it does, the commit fails and nothing is written; the writer must retry. What counts
# MAGIC as a conflict depends on the table: by **file** (two MERGEs that touch different rows of the same file
# MAGIC still collide) or, with **deletion vectors** on, by **row** (Databricks' row-level concurrency).
# MAGIC
# MAGIC | Case | Table | The two MERGEs | Expected |
# MAGIC |---|---|---|---|
# MAGIC | C1 | deletion vectors on, 1 file | different rows | both commit (row-level concurrency) |
# MAGIC | C2 | deletion vectors on, 1 file | the same rows | one fails with a concurrency error |
# MAGIC | C3 | deletion vectors off, 1 file | different rows | one fails: same file |
# MAGIC | C4 | C3's table | the loser, retried alone | commits; both changes present |
# MAGIC
# MAGIC **Part 2 — constraints.** `CHECK` and `NOT NULL` are enforced by Delta on every write;
# MAGIC `PRIMARY KEY` is accepted but **not** enforced (as in Snowflake). Shown on copies of `silver.orders`
# MAGIC and `order_items` with the production constraints.

# COMMAND ----------

# MAGIC %run ../databricks/silver/_merge_orders

# COMMAND ----------

import threading
import time
from concurrent.futures import ThreadPoolExecutor

DX = f"{SILVER}.dx"
GROUP = 1_000  # rows each MERGE changes


def first_line(error):
    text = str(error).strip()
    return text.splitlines()[0][:300] if text else type(error).__name__


def num_files(table):
    return spark.sql(f"DESCRIBE DETAIL {table}").first().numFiles


def stock_copy(name, deletion_vectors):
    """Silver's 34,348 stock rows in ONE file, so two MERGEs on different rows still share a file."""
    table = f"{DX}_{name}"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(
        f"""CREATE TABLE {table}
            TBLPROPERTIES ('delta.enableDeletionVectors' = '{str(deletion_vectors).lower()}')
            AS SELECT sku_key, stock_qty, seq FROM {SILVER}.inventory"""
    )
    spark.sql(f"OPTIMIZE {table}")
    return table


# The changes both MERGEs apply: each row one sale newer. A separate table, so a MERGE's source read does
# not touch the table being written.
CHANGES = f"{DX}_changes"
spark.sql(f"DROP TABLE IF EXISTS {CHANGES}")
spark.sql(
    f"""CREATE TABLE {CHANGES} AS
        SELECT sku_key, stock_qty - 1 AS stock_qty, seq + 1 AS seq,
               row_number() OVER (ORDER BY sku_key) AS _n
        FROM {SILVER}.inventory"""
)
TOTAL = spark.table(CHANGES).count()
FIRST = f"_n <= {GROUP}"
LAST = f"_n > {TOTAL - GROUP}"


def merge_sql(table, rows):
    return (
        f"MERGE INTO {table} AS t "
        f"USING (SELECT sku_key, stock_qty, seq FROM {CHANGES} WHERE {rows}) AS s "
        f"ON t.sku_key = s.sku_key "
        f"WHEN MATCHED AND s.seq > t.seq THEN UPDATE SET t.stock_qty = s.stock_qty, t.seq = s.seq"
    )


def race(table, rows_a, rows_b, attempts=3):
    """Start two MERGEs at the same instant from two threads. Repeats (on a fresh state) until the two
    provably overlapped: one failed, or both commits read the same version."""
    for attempt in range(1, attempts + 1):
        start = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first().version
        barrier = threading.Barrier(2)

        def one(rows, barrier=barrier):
            barrier.wait()
            began = time.time()
            try:
                spark.sql(merge_sql(table, rows))
                return {"rows": rows, "result": "committed", "seconds": round(time.time() - began, 1)}
            except Exception as e:
                return {"rows": rows, "result": first_line(e), "seconds": round(time.time() - began, 1)}

        with ThreadPoolExecutor(2) as pool:
            outcomes = [f.result() for f in [pool.submit(one, rows_a), pool.submit(one, rows_b)]]
        commits = (
            spark.sql(f"DESCRIBE HISTORY {table}")
            .where(f"version > {start} AND operation = 'MERGE'")
            .select("version", "readVersion", "isolationLevel", "operationMetrics")
            .orderBy("version")
            .collect()
        )
        failed = [o for o in outcomes if o["result"] != "committed"]
        overlapped = bool(failed) or len({c.readVersion for c in commits}) == 1
        print(f"attempt {attempt}: overlapped={overlapped}")
        for o in outcomes:
            print(f"  {o['rows']:>14}: {o['result']}  ({o['seconds']} s)")
        for c in commits:
            print(f"  v{c.version} read v{c.readVersion} {c.isolationLevel} "
                  f"updated {c.operationMetrics.get('numTargetRowsUpdated')}")
        if overlapped:
            return outcomes, commits
        spark.sql(f"RESTORE TABLE {table} TO VERSION AS OF {start}")
    raise RuntimeError(f"the two MERGEs never overlapped in {attempts} attempts — they ran one at a time")


def is_conflict(result):
    return "concurrent" in result.lower() or "conflict" in result.lower()

# COMMAND ----------

# MAGIC %md
# MAGIC ## C1 — deletion vectors on, the two MERGEs change different rows

# COMMAND ----------

dv = stock_copy("dv", deletion_vectors=True)
print(f"{dv}: {num_files(dv)} file(s), {spark.table(dv).count():,} rows")
assert num_files(dv) == 1
c1, c1_commits = race(dv, FIRST, LAST)
assert all(o["result"] == "committed" for o in c1), "row-level concurrency did not resolve different rows"
assert len(c1_commits) == 2 and c1_commits[0].readVersion == c1_commits[1].readVersion

# COMMAND ----------

# MAGIC %md
# MAGIC ## C2 — deletion vectors on, the two MERGEs change the same rows

# COMMAND ----------

dv2 = stock_copy("dv2", deletion_vectors=True)
assert num_files(dv2) == 1
c2, _ = race(dv2, FIRST, FIRST)
results = [o["result"] for o in c2]
assert results.count("committed") == 1 and is_conflict(next(r for r in results if r != "committed"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## C3 — deletion vectors off: different rows, but one file

# COMMAND ----------

plain = stock_copy("plain", deletion_vectors=False)
assert num_files(plain) == 1
c3, _ = race(plain, FIRST, LAST)
results = [o["result"] for o in c3]
assert results.count("committed") == 1 and is_conflict(next(r for r in results if r != "committed"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## C4 — the production answer to a conflict: retry the loser
# MAGIC
# MAGIC Safe here because the MERGE is idempotent by its own condition (`s.seq > t.seq`), like every Silver
# MAGIC write: a retry of something that already committed changes nothing.

# COMMAND ----------

loser = next(o["rows"] for o in c3 if o["result"] != "committed")
spark.sql(merge_sql(plain, loser))
changed = spark.table(plain).alias("t").join(spark.table(CHANGES).alias("c"), "sku_key").where(
    "t.seq = c.seq AND t.stock_qty = c.stock_qty"
).count()
print(f"rows carrying a change after the retry: {changed:,} (expected {2 * GROUP:,})")
assert changed == 2 * GROUP
spark.sql(merge_sql(plain, loser))
again = spark.sql(f"DESCRIBE HISTORY {plain} LIMIT 1").first()
updated = again.operationMetrics.get("numTargetRowsUpdated")
print(f"retried twice: last commit {again.operation} updated {updated}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 — constraints, on copies of `silver.orders` and `silver.order_items`

# COMMAND ----------

orders_copy, items_copy = f"{DX}_orders", f"{DX}_order_items"
for copy, source, rules in ((orders_copy, ORDERS, ORDER_CONSTRAINTS), (items_copy, ITEMS, ITEM_CONSTRAINTS)):
    spark.sql(f"DROP TABLE IF EXISTS {copy}")
    spark.sql(f"CREATE TABLE {copy} AS SELECT * FROM {source}")
    ensure_constraints(copy, rules)


def attempt(sql, table):
    """Run one write; (error or None, versions added, rows before, rows after)."""
    before_v = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first().version
    before_n = spark.table(table).count()
    try:
        spark.sql(sql)
        error = None
    except Exception as e:
        error = first_line(e)
    after_v = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first().version
    return error, after_v - before_v, before_n, spark.table(table).count()


one_order = spark.table(orders_copy).orderBy("order_id").first()


def order_row(order_id, status):
    """INSERT … SELECT of a real order, with its id and status replaced."""
    cols = spark.table(orders_copy).columns
    status_sql = "CAST(NULL AS STRING)" if status is None else f"'{status}'"
    values = [f"'{order_id}'" if c == "order_id" else status_sql if c == "status" else c for c in cols]
    return (
        f"INSERT INTO {orders_copy} ({', '.join(cols)}) SELECT {', '.join(values)} "
        f"FROM {orders_copy} WHERE order_id = '{one_order.order_id}'"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **K1 — a row that breaks a CHECK.** An order with status `lost`.

# COMMAND ----------

k1 = attempt(order_row("dx-lost", "lost"), orders_copy)
print(f"K1: {k1}")
assert k1[0] and k1[1] == 0 and k1[2] == k1[3], "a CHECK violation must fail the write and commit nothing"

# COMMAND ----------

# MAGIC %md
# MAGIC **K2 — one bad row in a 1,000-row MERGE.** 999 item prices raised by 1, one set to -1. All or nothing?

# COMMAND ----------

price_before = spark.table(items_copy).agg(F.round(F.sum("price"), 2)).first()[0]
k2 = attempt(
    f"""MERGE INTO {items_copy} t
        USING (SELECT order_id, order_item_id,
                      CASE WHEN rn = 1 THEN -1.0 ELSE price + 1 END AS price
               FROM (SELECT *, row_number() OVER (ORDER BY order_id, order_item_id) AS rn FROM {items_copy})
               WHERE rn <= 1000) s
        ON t.order_id = s.order_id AND t.order_item_id = s.order_item_id
        WHEN MATCHED THEN UPDATE SET t.price = s.price""",
    items_copy,
)
price_after = spark.table(items_copy).agg(F.round(F.sum("price"), 2)).first()[0]
print(f"K2: {k2}; price total before {price_before:,.2f} after {price_after:,.2f}")
assert k2[0] and k2[1] == 0 and price_before == price_after, "999 good rows must not land without the 1 bad"

# COMMAND ----------

# MAGIC %md
# MAGIC **K3 — a rule the existing rows break.** "Shipped no earlier than approved" — Olist breaks it on 1,359
# MAGIC orders where both are set. Delta checks every row before adding a rule. The number in the error also
# MAGIC tells how it counts NULLs: 1,359 if NULL passes; 3,156 if NULL fails (1,797 orders lack one of them).

# COMMAND ----------

k3 = attempt(
    f"ALTER TABLE {orders_copy} ADD CONSTRAINT shipped_after_approved CHECK (shipped_at >= approved_at)",
    orders_copy,
)
print(f"K3: {k3}")
assert k3[0] and k3[1] == 0
assert "delta.constraints.shipped_after_approved" not in {
    r.key for r in spark.sql(f"SHOW TBLPROPERTIES {orders_copy}").collect()
}

# COMMAND ----------

# MAGIC %md
# MAGIC **K4 — NULL.** Standard SQL lets a row through when a CHECK is NULL (unknown, not false). Delta?

# COMMAND ----------

k4 = attempt(order_row("dx-null", None), orders_copy)
print(f"K4: {k4}")
print("Delta treats a NULL check as a VIOLATION" if k4[0] else "Delta lets a NULL check through, as SQL does")

# COMMAND ----------

# MAGIC %md
# MAGIC **K5 — `PRIMARY KEY`: declared, accepted, not enforced.** Unity Catalog stores it (tools and the
# MAGIC optimizer may use it); Delta does not check it. Same in Snowflake — the key is a promise you keep.

# COMMAND ----------

spark.sql(f"ALTER TABLE {orders_copy} ALTER COLUMN order_id SET NOT NULL")
k5_add = attempt(f"ALTER TABLE {orders_copy} ADD CONSTRAINT dx_orders_pk PRIMARY KEY (order_id)", orders_copy)
print(f"K5 add primary key: {k5_add}")
k5 = attempt(order_row(one_order.order_id, one_order.status), orders_copy)
copies = spark.table(orders_copy).where(F.col("order_id") == one_order.order_id).count()
print(f"K5 duplicate insert: {k5}; rows with that order_id now: {copies}")
assert k5_add[0] is None, "UC refused the PRIMARY KEY — record why"
assert k5[0] is None and copies == 2, "the duplicate was refused — the key IS enforced here; record it"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Every constraint on the copies, as Delta stores them

# COMMAND ----------

for table in (orders_copy, items_copy):
    for r in spark.sql(f"SHOW TBLPROPERTIES {table}").where("key LIKE 'delta.constraints.%'").collect():
        print(f"{table}: {r.key} = {r.value}")
extended = spark.sql(f"DESCRIBE TABLE EXTENDED {orders_copy}")
display(extended.where("col_name LIKE '%onstraint%' OR data_type LIKE '%PRIMARY%'"))