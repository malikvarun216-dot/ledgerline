"""Drill 3, B1 and B2: Gold built from an altered copy of RAW, compared with live Gold.

    python snowflake/experiments/drill3_raw_clones.py setup     # make both clones, print predictions
    python snowflake/experiments/drill3_raw_clones.py late      # B2 phase 2: the held rows arrive late
    python snowflake/experiments/drill3_raw_clones.py drop      # drop both clone schemas

Both clones are zero-copy clones of LEDGERLINE.RAW.SILVER_CHANGES, made by the CI role in
LEDGERLINE_CI (it may read RAW, never write it). Gold is then built from a clone with
`scripts/replay_gold.py --raw <schema>`; production's dbt source ignores the override (_sources.yml).

B1 DRILL3_RAW_DUP — a bridge range loaded twice: every row of the orders file and of the newest
   customer file inserted again, unchanged (what a pipe reload or a `COPY … FORCE` would do). Gold is
   keyed on (key, version), so the claim is: Gold equals live. RAW's own check C must see the copies.
B2 DRILL3_RAW_LATE — late arrival, on today's code (exp_06 ran on experiment models in S17):
   changes held back from the first build, then delivered with LOADED_AT = now:
     g1  200 orders whose ONLY change is held       → absent, then inserted
     g2  300 orders whose NEWEST change is held     → an old status, then the newest
     g3  300 orders whose second-newest change is held → the late OLDER change must lose (`>=` guard)
     gi  300 order-item rows                        → absent, then inserted
   The claim: after the late load, Gold equals live except `raw_loaded_at` (arrival time, truly
   later) on the g1 + g2 orders and the gi items.

Runs as LEDGERLINE_CI_SVC (role LEDGERLINE_CI_RUNNER).
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from sf import connect  # noqa: E402

from generators._common import load_local_env  # noqa: E402

DUP = "LEDGERLINE_CI.DRILL3_RAW_DUP"
LATE = "LEDGERLINE_CI.DRILL3_RAW_LATE"
RAW = "LEDGERLINE.RAW.SILVER_CHANGES"

HELD_SQL = f"""
CREATE OR REPLACE TABLE {LATE}.HELD AS
WITH o AS (
    SELECT r.*, RECORD:order_id::string AS oid, RECORD:_commit_version::int AS v,
           row_number() OVER (PARTITION BY RECORD:order_id::string
                              ORDER BY RECORD:_commit_version::int DESC) AS rn,
           count(*) OVER (PARTITION BY RECORD:order_id::string) AS n
    FROM {LATE}.SILVER_CHANGES r WHERE TABLE_NAME = 'orders'
),
g1 AS (SELECT 'g1' AS grp, * FROM o WHERE n = 1 ORDER BY hash(oid) LIMIT 200),
g2 AS (SELECT 'g2' AS grp, * FROM o WHERE n > 1 AND rn = 1 ORDER BY hash(oid) LIMIT 300),
g3 AS (SELECT 'g3' AS grp, * FROM o WHERE n > 1 AND rn = 2
         AND oid NOT IN (SELECT oid FROM g2) ORDER BY hash(oid) LIMIT 300),
gi AS (SELECT 'gi' AS grp, r.*, NULL AS oid, NULL AS v, NULL AS rn, NULL AS n
       FROM {LATE}.SILVER_CHANGES r WHERE TABLE_NAME = 'order_items'
       ORDER BY hash(SOURCE_FILE, SOURCE_ROW) LIMIT 300)
SELECT * FROM g1 UNION ALL SELECT * FROM g2 UNION ALL SELECT * FROM g3 UNION ALL SELECT * FROM gi
"""


def run(cur, sql: str):
    cur.execute(sql)
    return cur.fetchall()


def setup(cur) -> None:
    for schema in (DUP, LATE):
        run(cur, f"CREATE SCHEMA IF NOT EXISTS {schema}")
        run(cur, f"CREATE OR REPLACE TABLE {schema}.SILVER_CHANGES CLONE {RAW}")
    newest_customer_file = run(cur, f"SELECT max(SOURCE_FILE) FROM {RAW} WHERE TABLE_NAME = 'customer'")[0][0]
    run(cur, f"INSERT INTO {DUP}.SILVER_CHANGES SELECT * FROM {RAW} "
             f"WHERE TABLE_NAME = 'orders' OR SOURCE_FILE = '{newest_customer_file}'")
    dup = run(cur, f"SELECT count(*) FROM {DUP}.SILVER_CHANGES")[0][0]
    twice = run(cur, f"SELECT count(*) FROM (SELECT SOURCE_FILE, SOURCE_ROW FROM {DUP}.SILVER_CHANGES "
                     "GROUP BY 1, 2 HAVING count(*) > 1)")[0][0]
    print(f"B1 {DUP}: {dup:,} rows (RAW 794,427); rows loaded twice (check C would list): {twice:,}")
    print(f"   newest customer file doubled: {newest_customer_file}")

    run(cur, HELD_SQL)
    run(cur, f"DELETE FROM {LATE}.SILVER_CHANGES r USING {LATE}.HELD h "
             "WHERE r.SOURCE_FILE = h.SOURCE_FILE AND r.SOURCE_ROW = h.SOURCE_ROW")
    groups = run(cur, f"SELECT grp, count(*) FROM {LATE}.HELD GROUP BY 1 ORDER BY 1")
    left = run(cur, f"SELECT count(*) FROM {LATE}.SILVER_CHANGES")[0][0]
    print(f"B2 {LATE}: held {dict(groups)}; clone keeps {left:,} rows")
    print("   predicted after the first build: FCT_ORDERS 99,241 (200 g1 fewer),"
          " FCT_ORDER_ITEMS 112,350 (300 gi fewer)")
    print("   predicted after the late load: equal to live except raw_loaded_at on 500 orders and 300 items")


def late(cur) -> None:
    cols = [r[0] for r in run(cur, "SELECT column_name FROM LEDGERLINE.INFORMATION_SCHEMA.COLUMNS "
                                   "WHERE table_schema = 'RAW' AND table_name = 'SILVER_CHANGES' "
                                   "ORDER BY ordinal_position")]
    select = ", ".join("CURRENT_TIMESTAMP()" if c == "LOADED_AT" else f'"{c}"' for c in cols)
    n = run(cur, f"INSERT INTO {LATE}.SILVER_CHANGES ({', '.join(chr(34) + c + chr(34) for c in cols)}) "
                 f"SELECT {select} FROM {LATE}.HELD")[0][0]
    print(f"B2: {n:,} held rows delivered late (LOADED_AT = now)")


def drop(cur) -> None:
    for schema in (DUP, LATE):
        run(cur, f"DROP SCHEMA IF EXISTS {schema}")
        print(f"dropped {schema}")


def main(argv: list[str] | None = None) -> int:
    step = (argv if argv is not None else sys.argv[1:] or ["setup"])[0]
    load_local_env(ROOT / ".env")
    conn = connect("LEDGERLINE_CI_RUNNER", as_ci=True)
    try:
        cur = conn.cursor()
        cur.execute("USE WAREHOUSE LEDGERLINE_WH")
        {"setup": setup, "late": late, "drop": drop}[step](cur)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
