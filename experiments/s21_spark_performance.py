# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Performance on Databricks: layouts, data skipping, plans, system tables (Session 21)
# MAGIC
# MAGIC **The question:** the same 29,999,795 order lines (`samples.tpch.lineitem`, TPC-H's sample), one
# MAGIC ship date asked for. How many files does Delta read, depending on how the table was laid out?
# MAGIC Delta keeps the **min and max of each column per file** in its log; a file whose range cannot hold
# MAGIC the date is skipped without being opened (*data skipping*). The layout decides how narrow those
# MAGIC ranges are.
# MAGIC
# MAGIC - **A** four copies of the same rows:
# MAGIC   - `li_random` — shuffled: every file holds every date, nothing can be skipped (the bad baseline);
# MAGIC   - `li_part` — `PARTITIONED BY (l_shipdate)`: a folder per date (2,526 of them) — the classic
# MAGIC     over-partitioning mistake: perfect pruning, thousands of small files;
# MAGIC   - `li_zorder` — the shuffled copy after `OPTIMIZE … ZORDER BY (l_shipdate)`;
# MAGIC   - `li_liquid` — `CLUSTER BY (l_shipdate)` (liquid clustering) + `OPTIMIZE`.
# MAGIC   Per copy: files, average file size, and for one day the files read vs pruned (from the Query
# MAGIC   History API's metrics — the numbers the Query Profile shows).
# MAGIC - **B** Spark plans (`EXPLAIN FORMATTED`): a join to a 25-row table is broadcast; a join to the
# MAGIC   7.5 M-row `orders` is a shuffle join; a `BROADCAST` hint changes it; the partitioned copy's filter
# MAGIC   is a `PartitionFilters` entry; adaptive query execution (AQE) is on. And the skew of one key.
# MAGIC - **Z** system tables (**B?** in `coverage.md`): which of `system.billing`, `system.access`,
# MAGIC   `system.query`, `system.compute` this workspace lets its owner read — each a finding either way.
# MAGIC
# MAGIC Run by the human ("Run all") — the owner, because system tables are granted to admins, not to the
# MAGIC `ledgerline-ci` service principal (it sees only `system.ai`, S21). Scratch in `workspace.s21` (the
# MAGIC S11 rule). Predictions were written before the first run; a wrong one is the finding.

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient

S = "workspace.s21"
SRC = "samples.tpch.lineitem"
ROWS = 29_999_795
DAY = "1995-03-15"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S} COMMENT 'Session 21 scratch: layouts, data skipping, plans'")

results = []


def check(label, got, predicted, ok=None):
    ok = (got == predicted) if ok is None else ok
    results.append((label, got, predicted, ok))
    print(f"  {'ok   ' if ok else 'WRONG'} {label}: got {got!r}, predicted {predicted!r}")


def detail(t):
    d = spark.sql(f"DESCRIBE DETAIL {t}").collect()[0]
    return {"files": d.numFiles, "avg_mb": round(d.sizeInBytes / max(d.numFiles, 1) / 2**20, 2),
            "clustering": d.clusteringColumns, "partitions": d.partitionColumns}


w = WorkspaceClient()


def metrics(marker, since_ms, timeout=120):
    """Files read / pruned for the query whose text holds `marker`, from the Query History API."""
    from databricks.sdk.service.sql import QueryFilter, TimeRange

    flt = QueryFilter(query_start_time_range=TimeRange(start_time_ms=since_ms))
    end = time.time() + timeout
    while time.time() < end:
        for q in w.query_history.list(filter_by=flt, include_metrics=True, max_results=200).res or []:
            if marker in (q.query_text or "") and q.metrics and q.metrics.read_files_count is not None:
                m = q.metrics
                return {"read": m.read_files_count, "pruned": m.pruned_files_count,
                        "read_mb": round((m.read_bytes or 0) / 2**20, 1), "ms": m.total_time_ms}
        time.sleep(10)
    return None

# COMMAND ----------

# MAGIC %md ## Z. System tables — verify first (B?)

# COMMAND ----------

print("Z. system tables, as the workspace owner")
print("  schemas in system:", [r[0] for r in spark.sql("SHOW SCHEMAS IN system").collect()])
for t in ("system.billing.usage", "system.billing.list_prices", "system.access.audit",
          "system.query.history", "system.compute.warehouses", "system.lakeflow.job_run_timeline",
          "system.access.table_lineage"):
    try:
        n = spark.sql(f"SELECT count(*) FROM {t}").collect()[0][0]
        print(f"  readable  {t}: {n:,} rows")
    except Exception as e:
        print(f"  refused   {t}: {str(e).splitlines()[0][:160]}")

# Our own spend, if billing is readable: DBUs by product over the last 30 days ($0 on Free Edition,
# but the DBUs are still metered).
try:
    spark.sql("""SELECT billing_origin_product, usage_unit, round(sum(usage_quantity), 2) AS qty
                 FROM system.billing.usage WHERE usage_date >= current_date() - 30
                 GROUP BY 1, 2 ORDER BY qty DESC""").show(truncate=False)
except Exception as e:
    print(f"  billing by product refused: {str(e).splitlines()[0][:160]}")

# COMMAND ----------

# MAGIC %md ## A. One table, four layouts

# COMMAND ----------

print("\nA. build the four copies")
t0 = time.time()
spark.sql(f"CREATE OR REPLACE TABLE {S}.li_random AS SELECT * FROM {SRC} ORDER BY rand(42)")
spark.sql(f"CREATE OR REPLACE TABLE {S}.li_part PARTITIONED BY (l_shipdate) AS SELECT * FROM {SRC}")
spark.sql(f"CREATE OR REPLACE TABLE {S}.li_zorder SHALLOW CLONE {S}.li_random")
spark.sql(f"OPTIMIZE {S}.li_zorder ZORDER BY (l_shipdate)")
spark.sql(f"CREATE OR REPLACE TABLE {S}.li_liquid CLUSTER BY (l_shipdate) AS SELECT * FROM {S}.li_random")
spark.sql(f"OPTIMIZE {S}.li_liquid")
print(f"  built in {round(time.time() - t0)} s")

layouts = ("li_random", "li_part", "li_zorder", "li_liquid")
for name in layouts:
    n = spark.sql(f"SELECT count(*) FROM {S}.{name}").collect()[0][0]
    check(f"A0 {name} holds every row", n, ROWS)
info = {name: detail(f"{S}.{name}") for name in layouts}
for name, d in info.items():
    print(f"  {name}: {d}")
check("A1 li_part: more than 2,526 files (at least one per date)", info["li_part"]["files"] > 2526, True)
check("A2 li_part: average file under 1 MB (the small-file problem)", info["li_part"]["avg_mb"] < 1, True)
check("A3 li_liquid: clustering column recorded", info["li_liquid"]["clustering"], ["l_shipdate"])

# COMMAND ----------

print(f"\nA. one day ({DAY}) from each layout")
since = int(time.time() * 1000) - 5_000
answers, read = {}, {}
for name in layouts:
    marker = f"s21-A-{name}-{int(time.time())}"
    answers[name] = spark.sql(
        f"SELECT /* {marker} */ round(sum(l_extendedprice), 2) FROM {S}.{name} WHERE l_shipdate = '{DAY}'"
    ).collect()[0][0]
    read[name] = metrics(marker, since)
    print(f"  {name}: sum {answers[name]}, files {read[name]} of {info[name]['files']}")
check("A4 the same answer from all four", len(set(answers.values())), 1)
if all(read.values()):
    check("A5 li_random: reads >= 90% of its files",
          read["li_random"]["read"] >= 0.9 * info["li_random"]["files"], True)
    check("A6 li_part: reads only its date's files (<= 1% of files)",
          read["li_part"]["read"] <= 0.01 * info["li_part"]["files"], True)
    check("A7 li_zorder: reads <= 3 files", read["li_zorder"]["read"] <= 3, True)
    check("A8 li_liquid: reads <= 3 files", read["li_liquid"]["read"] <= 3, True)
else:
    print("  query metrics not found through the API: read files read / pruned in Query History by hand")

# COMMAND ----------

# MAGIC %md ## B. Spark plans: broadcast vs shuffle, partition pruning, AQE, skew

# COMMAND ----------


def plan(sql):
    return "\n".join(r[0] for r in spark.sql(f"EXPLAIN FORMATTED {sql}").collect())


print("\nB. plans")
small = plan(f"""SELECT n.n_name, sum(l.l_quantity) FROM {S}.li_liquid l
    JOIN samples.tpch.supplier s ON l.l_suppkey = s.s_suppkey
    JOIN samples.tpch.nation n ON s.s_nationkey = n.n_nationkey GROUP BY 1""")
big = plan(f"""SELECT o.o_orderpriority, sum(l.l_quantity) FROM {S}.li_liquid l
    JOIN samples.tpch.orders o ON l.l_orderkey = o.o_orderkey GROUP BY 1""")
hinted = plan(f"""SELECT /*+ BROADCAST(o) */ o.o_orderpriority, sum(l.l_quantity) FROM {S}.li_liquid l
    JOIN samples.tpch.orders o ON l.l_orderkey = o.o_orderkey GROUP BY 1""")
part = plan(f"SELECT sum(l_extendedprice) FROM {S}.li_part WHERE l_shipdate = '{DAY}'")
check("B1 a 25-row nation table is broadcast", "BroadcastHashJoin" in small, True)
check("B2 7.5 M orders: a shuffle join (SortMerge or ShuffledHash)",
      "SortMergeJoin" in big or "ShuffledHashJoin" in big, True)
check("B3 the BROADCAST hint makes it a broadcast", "BroadcastHashJoin" in hinted, True)
check("B4 the partitioned copy's filter is a partition filter", "PartitionFilters: [" in part
      and "l_shipdate" in part.split("PartitionFilters: [", 1)[1].split("]", 1)[0], True)
check("B5 the plan is adaptive (AQE on)", "AdaptiveSparkPlan" in big, True)
print("\n--- the shuffle join's plan, first 40 lines ---")
print("\n".join(big.splitlines()[:40]))
try:
    print("\n  spark.sql.adaptive.enabled =", spark.conf.get("spark.sql.adaptive.enabled"))
    print("  spark.sql.autoBroadcastJoinThreshold =", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
except Exception as e:
    print(f"  conf read refused on serverless: {str(e).splitlines()[0][:160]}")

print("\nB6 skew: rows per l_returnflag (a join or group on it puts each value on one task)")
spark.sql(f"""SELECT l_returnflag, count(*) AS n, round(100 * count(*) / {ROWS}, 1) AS pct
              FROM {S}.li_liquid GROUP BY 1 ORDER BY n DESC""").show()

# COMMAND ----------

# MAGIC %md ## Result

# COMMAND ----------

wrong = [r for r in results if not r[3]]
print(f"{len(results) - len(wrong)}/{len(results)} as predicted")
for label, got, predicted, _ in wrong:
    print(f"  WRONG {label}: got {got!r}, predicted {predicted!r}")