"""Session 25: write-audit-publish proven on a copy of production, in LEDGERLINE_CI.

    python snowflake/experiments/s25_wap.py setup        # RAW clone, 300 newest order changes held back
    python snowflake/experiments/s25_wap.py hashes       # rows + HASH_AGG(*) of every table in S25_WAP
    python snowflake/experiments/s25_wap.py bad          # a bad night: 50 items whose order never came
    python snowflake/experiments/s25_wap.py good         # bad rows removed, the 300 held changes arrive
    python snowflake/experiments/s25_wap.py drop         # drop the three schemas

The published side is LEDGERLINE_CI.S25_WAP (built from the clone by replay_gold.py --build-only),
its audit schema S25_WAP_AUDIT, the RAW clone S25_RAW. Claims:
  W1 a bad night stops at the audit: publish_gold.py exits 1, S25_WAP's hashes do not move, and the
     audit schema holds FCT_ORDER_ITEMS + 50;
  W2 the next night starts from what is published: after `good`, the 50 bad items are nowhere,
     although the failed audit held them;
  W3 a green night is published: S25_WAP then equals live GOLD except RAW_LOADED_AT on the 300 orders.

Runs as LEDGERLINE_CI_SVC (role LEDGERLINE_CI_RUNNER): reads LEDGERLINE, writes only LEDGERLINE_CI.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from sf import connect  # noqa: E402

from generators._common import load_local_env  # noqa: E402

RAW = "LEDGERLINE.RAW.SILVER_CHANGES"
CLONE = "LEDGERLINE_CI.S25_RAW"
PUBLISHED = "S25_WAP"
BAD_FILE = "s25_wap/bad_night.parquet"

HELD_SQL = f"""
CREATE OR REPLACE TABLE {CLONE}.HELD AS
WITH o AS (
    SELECT r.*, row_number() OVER (PARTITION BY RECORD:order_id::string
                                   ORDER BY RECORD:_commit_version::int DESC) AS rn,
           count(*) OVER (PARTITION BY RECORD:order_id::string) AS n
    FROM {CLONE}.SILVER_CHANGES r WHERE TABLE_NAME = 'orders'
)
SELECT * EXCLUDE (rn, n) FROM o WHERE n > 1 AND rn = 1 ORDER BY hash(RECORD:order_id::string) LIMIT 300
"""


def run(cur, sql: str):
    cur.execute(sql)
    return cur.fetchall()


def columns(cur) -> list[str]:
    return [r[0] for r in run(cur, "SELECT column_name FROM LEDGERLINE.INFORMATION_SCHEMA.COLUMNS "
                                   "WHERE table_schema = 'RAW' AND table_name = 'SILVER_CHANGES' "
                                   "ORDER BY ordinal_position")]


def setup(cur) -> None:
    run(cur, f"CREATE SCHEMA IF NOT EXISTS {CLONE}")
    run(cur, f"CREATE OR REPLACE TABLE {CLONE}.SILVER_CHANGES CLONE {RAW}")
    run(cur, HELD_SQL)
    run(cur, f"DELETE FROM {CLONE}.SILVER_CHANGES r USING {CLONE}.HELD h "
             "WHERE r.SOURCE_FILE = h.SOURCE_FILE AND r.SOURCE_ROW = h.SOURCE_ROW")
    held = run(cur, f"SELECT count(*) FROM {CLONE}.HELD")[0][0]
    left = run(cur, f"SELECT count(*) FROM {CLONE}.SILVER_CHANGES")[0][0]
    print(f"{CLONE}: {left:,} rows (RAW 794,427), {held} newest order changes held back")


def hashes(cur) -> None:
    for schema in (PUBLISHED, f"{PUBLISHED}_AUDIT"):
        tables = run(cur, "SELECT table_name FROM LEDGERLINE_CI.INFORMATION_SCHEMA.TABLES "
                          f"WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE' "
                          "AND (table_name LIKE 'FCT%' OR table_name LIKE '%CURRENT' OR table_name IN "
                          "('DELIVERY_PROMISE','LATE_VS_RATING','SELLER_SCORECARD','COMPLAINT_TRENDS',"
                          "'REVIEW_LABELS')) ORDER BY 1")
        for (name,) in tables:
            n, h = run(cur, f'SELECT count(*), HASH_AGG(*) FROM LEDGERLINE_CI.{schema}."{name}"')[0]
            print(f"{schema:16} {name:20} {n:>9,} {h}")


def bad(cur) -> None:
    """50 order items for orders that never arrive: fails relationships fct_order_items -> fct_orders
    and reconcile_sold_vs_decrements, the same tests B5's incomplete night failed."""
    cols = columns(cur)
    select = ", ".join(
        "CURRENT_TIMESTAMP()" if c == "LOADED_AT"
        else f"'{BAD_FILE}'" if c == "SOURCE_FILE"
        else "OBJECT_INSERT(RECORD, 'order_id', 's25wap' || SOURCE_ROW, TRUE)" if c == "RECORD"
        else f'"{c}"'
        for c in cols
    )
    quoted = ", ".join(f'"{c}"' for c in cols)
    n = run(cur, f"INSERT INTO {CLONE}.SILVER_CHANGES ({quoted}) SELECT {select} FROM {CLONE}.SILVER_CHANGES "
                 "WHERE TABLE_NAME = 'order_items' ORDER BY hash(SOURCE_FILE, SOURCE_ROW) LIMIT 50")[0][0]
    print(f"bad night: {n} order items for orders that do not exist (SOURCE_FILE {BAD_FILE})")


def good(cur) -> None:
    gone = run(cur, f"DELETE FROM {CLONE}.SILVER_CHANGES WHERE SOURCE_FILE = '{BAD_FILE}'")[0][0]
    cols = columns(cur)
    select = ", ".join("CURRENT_TIMESTAMP()" if c == "LOADED_AT" else f'"{c}"' for c in cols)
    quoted = ", ".join(f'"{c}"' for c in cols)
    n = run(cur, f"INSERT INTO {CLONE}.SILVER_CHANGES ({quoted}) SELECT {select} FROM {CLONE}.HELD")[0][0]
    print(f"good night: {gone} bad rows removed from RAW, {n} held order changes delivered (LOADED_AT = now)")


def drop(cur) -> None:
    for schema in (CLONE, f"LEDGERLINE_CI.{PUBLISHED}", f"LEDGERLINE_CI.{PUBLISHED}_AUDIT"):
        run(cur, f"DROP SCHEMA IF EXISTS {schema}")
        print(f"dropped {schema}")


def main(argv: list[str] | None = None) -> int:
    step = (argv if argv is not None else sys.argv[1:] or ["hashes"])[0]
    load_local_env(ROOT / ".env")
    conn = connect("LEDGERLINE_CI_RUNNER", as_ci=True)
    try:
        cur = conn.cursor()
        cur.execute("USE WAREHOUSE LEDGERLINE_WH")
        {"setup": setup, "hashes": hashes, "bad": bad, "good": good, "drop": drop}[step](cur)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
