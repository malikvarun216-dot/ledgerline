"""Session 21: performance and cost on Snowflake, measured on copies of the TPC-H sample.

    python snowflake/experiments/s21_performance.py            # copies, A, B, C, D, gold
    python snowflake/experiments/s21_performance.py A2         # later: after automatic clustering ran

Run after the Session 21 block of 05_scratch_schemas.sql (schema S21, warehouse LEDGERLINE_S21_WH).
Every number comes from GET_QUERY_OPERATOR_STATS — the Query Profile as rows — or from
INFORMATION_SCHEMA, never from a screenshot (decisions.md, "S21 measures performance …").

copies  two CTAS copies of TPCH_SF10.LINEITEM (59,986,052 rows): shuffled, and sorted by ship date;
        the shuffle's sort is also C1 (does a full sort spill on XSMALL?)
A       pruning: one ship date read from the sample, the shuffled copy, the sorted copy (partitions
        scanned / total); clustering depth of each; then CLUSTER BY on the shuffled copy
A2      (a later run) the shuffled copy after automatic clustering: depth, pruning, its credits;
        automatic clustering suspended
B       the three caches: metadata (a COUNT with the warehouse suspended), result (the same query
        twice), warehouse disk (cold, warm, after a suspend)
C       the same sort on XSMALL vs SMALL (scale up removes the spill); an exploding join
D       12 queries at once: max 1 cluster vs max 3 (scale out removes the queue)
gold    GOLD.FCT_ORDERS: why our own data cannot teach pruning

Predictions were written before the first run; a wrong one does not stop the run — it is the
finding. The warehouse is put back to XSMALL / max 3 and suspended in finally:. Runs as
LEDGERLINE_TRANSFORMER; writes only in LEDGERLINE.S21.
"""

from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from sf import connect  # noqa: E402

from generators._common import load_local_env  # noqa: E402

S = "LEDGERLINE.S21"
WH = "LEDGERLINE_S21_WH"
SAMPLE = "SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.LINEITEM"
SAMPLE_100 = "SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.LINEITEM"
SAMPLE_1 = "SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM"
ROWS = 59_986_052
DAY = "1995-03-15"

results: list[tuple[str, object, object, bool]] = []
xsmall_spill: dict[str, int] = {}  # C1's bytes, for C2


def check(label, got, predicted, ok=None):
    ok = (got == predicted) if ok is None else ok
    results.append((label, got, predicted, ok))
    print(f"  {'ok   ' if ok else 'WRONG'} {label}: got {got!r}, predicted {predicted!r}")


def one(cur, sql: str):
    cur.execute(sql)
    return cur.fetchone()[0]


def timed(cur, sql: str) -> tuple[str, float]:
    """Run `sql`; (query id, seconds on the client's clock)."""
    start = time.time()
    cur.execute(sql)
    cur.fetchall()
    return cur.sfqid, round(time.time() - start, 1)


def ops(cur, qid: str) -> list[tuple[str, dict]]:
    """The Query Profile of `qid`, one (operator type, statistics) per operator."""
    cur.execute(f"SELECT operator_type, operator_statistics FROM TABLE(GET_QUERY_OPERATOR_STATS('{qid}'))")
    return [(t, json.loads(s) if isinstance(s, str) else (s or {})) for t, s in cur.fetchall()]


def scan(cur, qid: str) -> dict:
    """Pruning and IO of the query's table scans, summed: scanned, total, percent from cache.

    `percentage_scanned_from_cache` is a FRACTION (0-1) despite its name: run 1 read a warm 1.0 as 1%
    and called B4 wrong. Converted to a percent here."""
    scanned = total = 0
    cache = []
    for t, st in ops(cur, qid):
        if t == "TableScan":
            scanned += st.get("pruning", {}).get("partitions_scanned", 0)
            total += st.get("pruning", {}).get("partitions_total", 0)
            cache.append(round(100 * st.get("io", {}).get("percentage_scanned_from_cache", 0.0), 1))
    return {"scanned": scanned, "total": total, "cache": max(cache) if cache else None}


def spilled(cur, qid: str) -> tuple[int, int]:
    """Bytes spilled to local disk and to remote storage, over every operator."""
    local = remote = 0
    for _, st in ops(cur, qid):
        local += st.get("spilling", {}).get("bytes_spilled_local_storage", 0)
        remote += st.get("spilling", {}).get("bytes_spilled_remote_storage", 0)
    return local, remote


def depth(cur, table: str) -> dict:
    info = json.loads(one(cur, f"SELECT SYSTEM$CLUSTERING_INFORMATION('{table}', '(L_SHIPDATE)')"))
    return {k: info[k] for k in ("total_partition_count", "average_overlaps", "average_depth")}


def suspend(cur) -> str:
    try:
        cur.execute(f"ALTER WAREHOUSE {WH} SUSPEND")
    except Exception as e:  # already suspended is an error, not a problem
        print(f"        suspend: {str(e).splitlines()[0][:120]}")
    for _ in range(30):
        cur.execute(f"SHOW WAREHOUSES LIKE '{WH}'")
        cols = [d[0].lower() for d in cur.description]
        state = cur.fetchone()[cols.index("state")]
        if state == "SUSPENDED":
            return state
        time.sleep(2)
    return state


def wh_state(cur) -> str:
    cur.execute(f"SHOW WAREHOUSES LIKE '{WH}'")
    cols = [d[0].lower() for d in cur.description]
    return cur.fetchone()[cols.index("state")]


def copies(c) -> None:
    print("\ncopies (and C1: a full sort on XSMALL)")
    qid, secs = timed(
        c, f"CREATE OR REPLACE TABLE {S}.LINEITEM_RANDOM AS SELECT * FROM {SAMPLE} ORDER BY RANDOM(42)"
    )
    local, remote = spilled(c, qid)
    print(f"        shuffle CTAS: {secs} s, spilled local {local:,} B, remote {remote:,} B (query {qid})")
    xsmall_spill["local"] = local
    check("C1 a 60 M-row sort on XSMALL spills to local disk", local > 0, True)
    _, secs = timed(
        c, f"CREATE OR REPLACE TABLE {S}.LINEITEM_SORTED AS SELECT * FROM {SAMPLE} ORDER BY L_SHIPDATE"
    )
    print(f"        sorted CTAS: {secs} s")
    check(
        "copies hold every row",
        (
            one(c, f"SELECT COUNT(*) FROM {S}.LINEITEM_RANDOM"),
            one(c, f"SELECT COUNT(*) FROM {S}.LINEITEM_SORTED"),
        ),
        (ROWS, ROWS),
    )


def part_a(c) -> None:
    print("\nA. pruning and clustering depth")
    c.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    q = "SELECT SUM(L_EXTENDEDPRICE) FROM {} WHERE L_SHIPDATE = '" + DAY + "'"
    seen = {}
    for name, table in (
        ("sample", SAMPLE),
        ("random", f"{S}.LINEITEM_RANDOM"),
        ("sorted", f"{S}.LINEITEM_SORTED"),
    ):
        qid, secs = timed(c, q.format(table))
        seen[name] = scan(c, qid) | {"secs": secs, "sum": one(c, q.format(table))}
        print(f"        {name}: {seen[name]}")
    check("A1 same answer from all three", len({v["sum"] for v in seen.values()}), 1)
    r, s = seen["random"], seen["sorted"]
    check("A2 shuffled copy: one day reads >= 95% of partitions", r["scanned"] >= 0.95 * r["total"], True)
    check("A3 sorted copy: one day reads <= 3 partitions", s["scanned"] <= 3, True)
    check(
        "A4 the sample (loaded by order key) prunes poorly too: >= 50% read",
        seen["sample"]["scanned"] >= 0.5 * seen["sample"]["total"],
        True,
    )
    dr, ds = depth(c, f"{S}.LINEITEM_RANDOM"), depth(c, f"{S}.LINEITEM_SORTED")
    print(f"        depth random {dr}\n        depth sorted {ds}")
    check("A5 sorted copy: average depth <= 2", ds["average_depth"] <= 2, True)
    # Run 1 predicted ">= 50" and was wrong: the copy has only 48 partitions, so 50 was out of reach.
    # The worst possible depth is the partition count (every partition overlaps every other).
    check(
        "A6 shuffled copy: depth = its partition count (the worst possible)",
        dr["average_depth"],
        float(dr["total_partition_count"]),
    )
    c.execute(f"ALTER TABLE {S}.LINEITEM_RANDOM CLUSTER BY (L_SHIPDATE)")
    c.execute(f"SHOW TABLES LIKE 'LINEITEM_RANDOM' IN SCHEMA {S}")
    cols = [d[0].lower() for d in c.description]
    row = c.fetchone()
    auto = row[cols.index("automatic_clustering")]
    print(f"        cluster_by {row[cols.index('cluster_by')]!r}, automatic_clustering {auto!r}")
    check("A7 automatic clustering ON after CLUSTER BY", auto, "ON")


def part_a2(c) -> None:
    print("\nA2. after automatic clustering")
    c.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    dr = depth(c, f"{S}.LINEITEM_RANDOM")
    print(f"        depth random now {dr}")
    qid, secs = timed(c, f"SELECT SUM(L_EXTENDEDPRICE) FROM {S}.LINEITEM_RANDOM WHERE L_SHIPDATE = '{DAY}'")
    r = scan(c, qid)
    print(f"        one day: {r}, {secs} s")
    check("A8 depth fell below 10", dr["average_depth"] < 10, True)
    check("A9 one day now reads <= 10% of partitions", r["scanned"] <= 0.1 * r["total"], True)
    try:
        c.execute(f"""SELECT start_time, end_time, credits_used, num_bytes_reclustered, num_rows_reclustered
            FROM TABLE(LEDGERLINE.INFORMATION_SCHEMA.AUTOMATIC_CLUSTERING_HISTORY(
              DATE_RANGE_START => DATEADD('day', -2, CURRENT_TIMESTAMP()),
              TABLE_NAME => '{S}.LINEITEM_RANDOM')) ORDER BY start_time""")
        rows = c.fetchall()
        for row in rows:
            print(f"        {row}")
        print(f"        credits: {sum(float(r[2]) for r in rows):.3f} over {len(rows)} runs")
    except Exception as e:
        print(f"        clustering history refused: {str(e).splitlines()[0][:160]}")
    c.execute(f"ALTER TABLE {S}.LINEITEM_RANDOM SUSPEND RECLUSTER")
    c.execute(f"SHOW TABLES LIKE 'LINEITEM_RANDOM' IN SCHEMA {S}")
    cols = [d[0].lower() for d in c.description]
    check("A10 automatic clustering suspended", c.fetchone()[cols.index("automatic_clustering")], "OFF")


def part_b(c) -> None:
    print("\nB. the three caches")
    table = f"{S}.LINEITEM_SORTED"
    suspend(c)
    c.execute("ALTER SESSION SET USE_CACHED_RESULT = TRUE")
    n = one(c, f"SELECT COUNT(*) FROM {table}")
    mx = one(c, f"SELECT MAX(L_SHIPDATE) FROM {table}")
    print(f"        COUNT {n:,}, MAX ship date {mx}")
    check(
        "B1 metadata cache: COUNT and MAX answered with the warehouse still SUSPENDED",
        wh_state(c),
        "SUSPENDED",
    )

    agg = f"SELECT L_RETURNFLAG, L_LINESTATUS, SUM(L_QUANTITY), AVG(L_DISCOUNT) FROM {table} GROUP BY 1, 2"
    _, s1 = timed(c, agg)
    q2, s2 = timed(c, agg)
    t2 = [t for t, _ in ops(c, q2)]
    print(f"        result cache: first {s1} s, second {s2} s; second's operators {t2}")
    check(
        "B2 result cache: the second run is a different query id but scans nothing", scan(c, q2)["total"], 0
    )

    c.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    suspend(c)
    cold, sc = timed(c, agg)
    warm, sw = timed(c, agg)
    suspend(c)
    again, sa = timed(c, agg)
    pc, pw, pa = (scan(c, q)["cache"] for q in (cold, warm, again))
    print(f"        warehouse cache: cold {pc}% {sc} s, warm {pw}% {sw} s, after suspend {pa}% {sa} s")
    check("B3 cold: < 10% from cache", pc < 10, True)
    check("B4 warm: > 90% from cache", pw > 90, True)
    check("B5 after a suspend the disk cache is gone: < 10%", pa < 10, True)


def part_c(c) -> None:
    print("\nC. scale up removes the spill; an exploding join")
    c.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    c.execute(f"ALTER WAREHOUSE {WH} SET WAREHOUSE_SIZE = SMALL")
    qid, secs = timed(
        c, f"CREATE OR REPLACE TABLE {S}.SORT_ON_SMALL AS SELECT * FROM {SAMPLE} ORDER BY RANDOM(42)"
    )
    local, remote = spilled(c, qid)
    print(f"        the same shuffle on SMALL: {secs} s, spilled local {local:,} B, remote {remote:,} B")
    c.execute(f"ALTER WAREHOUSE {WH} SET WAREHOUSE_SIZE = XSMALL")
    c.execute(f"DROP TABLE {S}.SORT_ON_SMALL")
    if "local" in xsmall_spill:
        check("C2 SMALL spills less than XSMALL did", local < xsmall_spill["local"], True)

    a = f"(SELECT L_RETURNFLAG FROM {SAMPLE_1} WHERE L_SHIPDATE = '{DAY}')"
    b = f"(SELECT L_RETURNFLAG FROM {SAMPLE_1} WHERE L_SHIPDATE = '1995-03-16')"
    expect = one(
        c,
        f"""SELECT SUM(x.n * y.n) FROM (SELECT L_RETURNFLAG f, COUNT(*) n FROM {a} GROUP BY 1) x
        JOIN (SELECT L_RETURNFLAG f, COUNT(*) n FROM {b} GROUP BY 1) y ON x.f = y.f""",
    )
    rows_in = one(c, f"SELECT (SELECT COUNT(*) FROM {a}) + (SELECT COUNT(*) FROM {b})")
    joined = f"{a} x JOIN {b} y ON x.L_RETURNFLAG = y.L_RETURNFLAG"

    def join_out(qid: str) -> int | None:
        join = [st for t, st in ops(c, qid) if t == "Join"]
        return join[0].get("output_rows") if join else None

    # The rows themselves: the join really emits left x right per flag.
    keep = f"CREATE OR REPLACE TEMPORARY TABLE {S}.EXPLODED AS SELECT x.L_RETURNFLAG FROM {joined}"
    qid, secs = timed(c, keep)
    out = join_out(qid)
    print(f"        join on the flag (rows kept): {rows_in:,} rows in, {out:,} out ({secs} s)")
    check("C3 join output = sum over flags of left x right", out, expect)
    # Run 1 measured only COUNT(*) and was wrong (19,904, not 3,144,254): Snowflake counted one side per
    # flag BEFORE the join (an Aggregate under the Join), so the join emitted far fewer rows. Same answer.
    qid, _ = timed(c, f"SELECT COUNT(*) FROM {joined}")
    counted = one(c, f"SELECT COUNT(*) FROM {joined}")
    print(f"        COUNT(*) of the same join: answer {counted:,}, the join emitted {join_out(qid):,} rows")
    check("C4 COUNT(*) still answers left x right", counted, expect)
    check("C4 ... but its join emits fewer rows (aggregated below the join)", join_out(qid) < expect, True)


def part_d(c) -> None:
    print("\nD. scale out: 12 queries at once, max 1 cluster vs max 3")
    q = (
        "SELECT L_SUPPKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) s FROM "
        + SAMPLE_100
        + " WHERE L_QUANTITY > {k} GROUP BY 1 ORDER BY 2 DESC LIMIT 5"
    )

    def burst(tag: str) -> float:
        def worker(k: int) -> None:
            conn = connect(role="LEDGERLINE_TRANSFORMER")
            try:
                cur = conn.cursor()
                cur.execute(f"USE WAREHOUSE {WH}")
                cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
                cur.execute(f"ALTER SESSION SET QUERY_TAG = '{tag}'")
                cur.execute(q.format(k=k))
                cur.fetchall()
            finally:
                conn.close()

        start = time.time()
        threads = [threading.Thread(target=worker, args=(k,)) for k in range(12)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return round(time.time() - start, 1)

    def history(tag: str) -> dict:
        c.execute(f"""SELECT COUNT(*), COUNT(DISTINCT cluster_number),
                ROUND(SUM(queued_overload_time) / 1000, 1), ROUND(MAX(total_elapsed_time) / 1000, 1)
            FROM TABLE(LEDGERLINE.INFORMATION_SCHEMA.QUERY_HISTORY_BY_WAREHOUSE(
              WAREHOUSE_NAME => '{WH}', RESULT_LIMIT => 200))
            WHERE query_tag = '{tag}' AND query_type = 'SELECT'""")
        n, clusters, queued, longest = c.fetchone()
        return {
            "queries": n,
            "clusters": clusters,
            "queued_s": float(queued or 0),
            "longest_s": float(longest or 0),
        }

    run = time.strftime("%H%M%S")
    c.execute(f"ALTER WAREHOUSE {WH} SET MAX_CLUSTER_COUNT = 1")
    wall1 = burst(f"s21-d-max1-{run}")
    c.execute(f"ALTER WAREHOUSE {WH} SET MAX_CLUSTER_COUNT = 3")
    suspend(c)
    wall3 = burst(f"s21-d-max3-{run}")
    time.sleep(5)  # query history is written a moment after the query ends
    h1, h3 = history(f"s21-d-max1-{run}"), history(f"s21-d-max3-{run}")
    print(f"        max 1: wall {wall1} s, {h1}\n        max 3: wall {wall3} s, {h3}")
    check("D1 max 1: every query on one cluster", h1["clusters"], 1)
    check("D2 max 1: queries waited in the queue (> 0 s)", h1["queued_s"] > 0, True)
    check("D3 max 3: more than one cluster used", h3["clusters"] > 1, True)
    check("D4 max 3: less total queueing than max 1", h3["queued_s"] < h1["queued_s"], True)


def part_gold(c) -> None:
    print("\ngold. our own data")
    c.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    qid, _ = timed(c, "SELECT COUNT(*) FROM LEDGERLINE.GOLD.FCT_ORDERS WHERE CREATED_AT::DATE = '2017-11-24'")
    g = scan(c, qid)
    print(f"        FCT_ORDERS (99,441 rows), one day: {g}")
    check("G1 FCT_ORDERS has <= 5 micro-partitions in all", g["total"] <= 5, True)


PARTS = {
    "copies": copies,
    "A": part_a,
    "A2": part_a2,
    "B": part_b,
    "C": part_c,
    "D": part_d,
    "gold": part_gold,
}
DEFAULT = ["copies", "A", "B", "C", "D", "gold"]


def main(argv: list[str]) -> int:
    chosen = argv or DEFAULT
    unknown = [p for p in chosen if p not in PARTS]
    if unknown:
        raise SystemExit(f"unknown part(s) {unknown}; choose from {list(PARTS)}")
    load_local_env()
    conn = connect(role="LEDGERLINE_TRANSFORMER")
    c = conn.cursor()
    try:
        c.execute(f"USE WAREHOUSE {WH}")
        c.execute("ALTER SESSION SET QUERY_TAG = 'ledgerline:s21_performance'")
        for p in chosen:
            PARTS[p](c)
    finally:
        try:
            c.execute(f"ALTER WAREHOUSE {WH} SET WAREHOUSE_SIZE = XSMALL MAX_CLUSTER_COUNT = 3")
            print(f"\n  warehouse after the run: {suspend(c)}, XSMALL, max 3")
        except Exception as e:
            print(f"  reset failed: {str(e).splitlines()[0][:160]}")
        conn.close()
    wrong = [r for r in results if not r[3]]
    print(f"\n{len(results) - len(wrong)}/{len(results)} as predicted")
    for label, got, predicted, _ in wrong:
        print(f"  WRONG {label}: got {got!r}, predicted {predicted!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
