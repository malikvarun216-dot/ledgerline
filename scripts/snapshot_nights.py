"""Bring the SCD2 dimension snapshots up to date, one Silver night per dbt run, oldest first.

    python scripts/snapshot_nights.py            # plan: which nights each snapshot would take
    python scripts/snapshot_nights.py --apply    # run them

Why one night per run (exp_05, Session 14): a dbt snapshot sees only the source's state at the
moment it runs. If a bridge export brings two new dims nights and one snapshot runs, the earlier
night's changes are never seen — history lost, no error. So for each snapshot this reads the newest
night it already holds (its `silver_night` column; 0 if the table does not exist yet) and the
newest night staging has, and runs `dbt snapshot --vars "{as_of_night: n}"` for each night in
between, in order. The first build and every later run are the same code path; a run with nothing
new does nothing.

Runs as LEDGERLINE_TRANSFORMER (scripts/sf.py + scripts/run_dbt.py, key pair).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from run_dbt import main as dbt  # noqa: E402
from sf import connect  # noqa: E402

from generators._common import load_local_env  # noqa: E402

# snapshot -> the staging view it follows
SNAPSHOTS = {
    "customer_history": "stg_customer",
    "product_history": "stg_product",
    "seller_history": "stg_seller",
}


def scalar(cur, sql: str) -> int:
    cur.execute(sql)
    return int(cur.fetchone()[0] or 0)


def plan(cur) -> dict[str, list[int]]:
    todo = {}
    for snap, stg in SNAPSHOTS.items():
        cur.execute(f"SHOW TABLES LIKE '{snap.upper()}' IN SCHEMA LEDGERLINE.SNAPSHOTS")
        exists = bool(cur.fetchall())
        done = scalar(cur, f"SELECT MAX(silver_night) FROM LEDGERLINE.SNAPSHOTS.{snap}") if exists else 0
        newest = scalar(cur, f"SELECT MAX(night) FROM LEDGERLINE.STAGING.{stg}")
        todo[snap] = list(range(done + 1, newest + 1))
        print(f"  {snap:<18} holds night {done}, staging has {newest}: to run {todo[snap] or 'nothing'}")
    return todo


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="run the nights (default: only print the plan)")
    args = parser.parse_args(argv)
    load_local_env(ROOT / ".env")

    conn = connect("LEDGERLINE_TRANSFORMER")
    try:
        cur = conn.cursor()
        cur.execute("USE WAREHOUSE LEDGERLINE_WH")
        todo = plan(cur)
    finally:
        conn.close()
    if not args.apply:
        print("plan only — run again with --apply")
        return 0

    for snap, nights in todo.items():
        for n in nights:
            print(f"\n--- {snap}: night {n}")
            if dbt(["snapshot", "--select", snap, "--vars", f"{{as_of_night: {n}}}"]) != 0:
                print(f"{snap} night {n} failed; later nights not run (they would skip it)")
                return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
