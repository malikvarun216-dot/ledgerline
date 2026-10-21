"""Session 19, experiment 2: a Dynamic Table vs a materialized view, on the same inventory events.

    python snowflake/experiments/s19_dynamic_mv.py

Run after s19_streams_tasks.py (it reads S19.INV_EVENTS_ALL, and compares with S19.INV_CURRENT, the
task's result). Both objects are kept up to date by Snowflake, not by us:

  MATERIALIZED VIEW  one table, no joins, no window functions; maintained in the background on
                     every change, and a query is always current (Snowflake adds what is not yet
                     maintained). Serverless upkeep: resource monitors do not see it.
  DYNAMIC TABLE      any query (joins, windows); refreshed by a warehouse to within TARGET_LAG,
                     INCREMENTAL when the query allows it, else FULL. Between refreshes it is stale.

E  the materialized view: what it refuses (the "newest event per SKU" query, a join); what it takes
   (an aggregate per SKU); a new batch visible at once
F  the dynamic table: the same "newest event per SKU" the task built by hand, declared; its refresh
   mode; stale right after a batch lands, current within the lag; = the task's result; a redelivered
   batch changes nothing; its refresh history

Predictions were written before the first run; a wrong one does not stop the run — it is the
finding. The dynamic table is SUSPENDED in finally: (a suspended one never refreshes) and checked.
Runs as LEDGERLINE_TRANSFORMER; writes only in LEDGERLINE.S19.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from sf import connect  # noqa: E402

from generators._common import load_local_env  # noqa: E402

S = "LEDGERLINE.S19"
LIVE = 34_348

results: list[tuple[str, object, object, bool]] = []


def check(label, got, predicted):
    ok = got == predicted
    results.append((label, got, predicted, ok))
    print(f"  {'ok   ' if ok else 'WRONG'} {label}: got {got!r}, predicted {predicted!r}")


def one(cur, sql: str):
    cur.execute(sql)
    return cur.fetchone()[0]


def attempt(cur, sql: str) -> str:
    try:
        cur.execute(sql)
        return "ok"
    except Exception as e:
        print(f"        refused: {str(e).splitlines()[0][:200]}")
        return "refused"


def land(cur, batch: int) -> int:
    cur.execute(f"INSERT INTO {S}.DT_LANDING SELECT * FROM {S}.INV_EVENTS_ALL WHERE batch_no = {batch}")
    return cur.rowcount


def expected_live(cur, upto: int) -> int:
    return one(
        cur,
        f"""SELECT COUNT_IF(op <> 'D') FROM (SELECT op FROM {S}.INV_EVENTS_ALL WHERE batch_no <= {upto}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY sku_key ORDER BY seq DESC) = 1)""",
    )


def wait_count(cur, sql: str, want: int, timeout: int = 300) -> tuple[int, int]:
    """Poll until `sql` returns `want`: (last value, seconds waited)."""
    start = time.time()
    got = one(cur, sql)
    while got != want and time.time() - start < timeout:
        time.sleep(10)
        got = one(cur, sql)
    return got, round(time.time() - start)


def main() -> int:
    load_local_env()
    conn = connect(role="LEDGERLINE_TRANSFORMER")
    c = conn.cursor()
    try:
        c.execute("USE WAREHOUSE LEDGERLINE_WH")
        c.execute(f"CREATE OR REPLACE TABLE {S}.DT_LANDING LIKE {S}.INV_EVENTS_ALL")
        newest = f"""SELECT sku_key, product_id, seller_id, op, seq, stock_qty, event_ts FROM {S}.DT_LANDING
            QUALIFY ROW_NUMBER() OVER (PARTITION BY sku_key ORDER BY seq DESC) = 1"""

        print("\nE. the materialized view")
        check(
            "E1 MV of the newest event per SKU (a window)",
            attempt(
                c,
                f"""CREATE OR REPLACE MATERIALIZED
            VIEW {S}.MV_NEWEST AS {newest}""",
            ),
            "refused",
        )
        check(
            "E2 MV with a join",
            attempt(
                c,
                f"""CREATE OR REPLACE MATERIALIZED VIEW {S}.MV_JOIN AS
            SELECT l.sku_key, l.seq, e.batch_no FROM {S}.DT_LANDING l
            JOIN {S}.INV_EVENTS_ALL e ON l.event_id = e.event_id""",
            ),
            "refused",
        )
        check(
            "E3 MV of an aggregate per SKU",
            attempt(
                c,
                f"""CREATE OR REPLACE MATERIALIZED VIEW
            {S}.MV_PER_SKU AS SELECT sku_key, COUNT(*) AS events, MAX(seq) AS max_seq
            FROM {S}.DT_LANDING GROUP BY sku_key""",
            ),
            "ok",
        )
        n1 = land(c, 1)
        check(
            "E4 MV current at once after a batch (sum of events)",
            one(c, f"SELECT SUM(events) FROM {S}.MV_PER_SKU"),
            n1,
        )

        print("\nF. the dynamic table")
        check(
            "F1 create (target lag 1 minute)",
            attempt(
                c,
                f"""CREATE OR REPLACE DYNAMIC TABLE
            {S}.DT_CURRENT TARGET_LAG = '1 minute' WAREHOUSE = LEDGERLINE_WH
            REFRESH_MODE = AUTO INITIALIZE = ON_CREATE AS {newest}""",
            ),
            "ok",
        )
        c.execute(f"SHOW DYNAMIC TABLES LIKE 'DT_CURRENT' IN SCHEMA {S}")
        cols = [d[0].lower() for d in c.description]
        row = c.fetchone()
        check("F2 refresh mode chosen by AUTO", row[cols.index("refresh_mode")], "INCREMENTAL")
        live_sql = f"SELECT COUNT_IF(op <> 'D') FROM {S}.DT_CURRENT"
        check("F3 after the initial refresh = answer key (batch 1)", one(c, live_sql), expected_live(c, 1))
        land(c, 2)
        check("F4 right after batch 2 lands: still batch 1 (stale)", one(c, live_sql), expected_live(c, 1))
        got, waited = wait_count(c, live_sql, expected_live(c, 2))
        print(f"        batch 2 visible after {waited} s")
        check("F4 within the lag: batch 2", got, expected_live(c, 2))
        land(c, 3)
        got, waited = wait_count(c, live_sql, LIVE)
        print(f"        batch 3 visible after {waited} s")
        check("F5 after batch 3: 34,348 live", got, LIVE)
        check(
            "F5 differs from the task's INV_CURRENT (sku, stock, seq, deleted)",
            one(
                c,
                f"""SELECT COUNT(*)
            FROM {S}.DT_CURRENT d FULL OUTER JOIN {S}.INV_CURRENT t ON d.sku_key = t.sku_key
            WHERE d.sku_key IS NULL OR t.sku_key IS NULL OR d.stock_qty IS DISTINCT FROM t.stock_qty
               OR d.seq IS DISTINCT FROM t.seq OR (d.op = 'D') <> t.is_deleted""",
            ),
            0,
        )
        before = one(c, f"SELECT HASH_AGG(*) FROM {S}.DT_CURRENT")
        land(c, 2)  # a redelivered batch: two identical rows per event, same seq
        c.execute(f"ALTER DYNAMIC TABLE {S}.DT_CURRENT REFRESH")  # synchronous: no waiting for the lag
        check(
            "F6 a redelivered batch changes nothing",
            one(c, f"SELECT HASH_AGG(*) FROM {S}.DT_CURRENT") == before,
            True,
        )
        c.execute(f"""SELECT refresh_action, COUNT(*) FROM TABLE(LEDGERLINE.INFORMATION_SCHEMA.
            DYNAMIC_TABLE_REFRESH_HISTORY(NAME => '{S}.DT_CURRENT')) WHERE state = 'SUCCEEDED' GROUP BY 1""")
        actions = dict(c.fetchall())
        print(f"        refresh actions: {actions}")
        check("F7 no FULL refresh after the initial one", actions.get("FULL", 0), 0)
    finally:
        try:
            c.execute(f"ALTER DYNAMIC TABLE IF EXISTS {S}.DT_CURRENT SUSPEND")
        except Exception as e:
            print(f"  SUSPEND failed: {str(e).splitlines()[0][:160]}")
        c.execute(f"SHOW DYNAMIC TABLES IN SCHEMA {S}")
        cols = [d[0].lower() for d in c.description]
        states = {r[cols.index("name")]: r[cols.index("scheduling_state")] for r in c.fetchall()}
        print(f"\n  dynamic tables after the run: {states}")
        check("every dynamic table suspended", all(v == "SUSPENDED" for v in states.values()), True)
        conn.close()
    wrong = [r for r in results if not r[3]]
    print(f"\n{len(results) - len(wrong)}/{len(results)} as predicted")
    for label, got, predicted, _ in wrong:
        print(f"  WRONG {label}: got {got!r}, predicted {predicted!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
