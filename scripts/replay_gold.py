"""Rebuild all of Gold from empty, out of RAW, into a throwaway CI schema — and compare it with live Gold.

    python scripts/replay_gold.py --schema DRILL3_REPLAY            # build, compare, drop the schema
    python scripts/replay_gold.py --schema DRILL3_REPLAY --keep     # keep it to look at afterwards
    python scripts/replay_gold.py --schema DRILL3_REPLAY --compare-only

Why (Drill 3, decisions.md "Drill 3 replays Gold from what Snowflake already holds"): live Gold was
grown night by night — snapshots one Silver night per run, facts by incremental MERGE on an arrival
mark. If those are honest, one replay from nothing gives the same tables. Any column that differs is
either run time leaking into the data (a column dated by when it was built) or state that only the
live history holds — both worth knowing before a real rebuild is ever needed.

The order a fresh build needs:
  1. staging views (the snapshots read them),
  2. the three snapshots, night 1 to the newest, one dbt run per night (`snapshot_nights.py --target ci`),
  3. everything else, unit tests left out (a unit test that mocks `this` needs the model to exist),
  4. the unit tests,
  5. the comparison: for each LEDGERLINE.GOLD / SNAPSHOTS table, row count and HASH_AGG of every
     column, on both sides. HASH_AGG ignores row order, so equal = the same rows.

Runs as LEDGERLINE_CI_SVC (role LEDGERLINE_CI_RUNNER, target `ci`): it may create schemas in
LEDGERLINE_CI and read LEDGERLINE, never write it. Pipeline roles see clear text through the mask,
so the comparison is on real values.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from run_dbt import main as dbt  # noqa: E402
from sf import connect  # noqa: E402
from snapshot_nights import main as snapshot_nights  # noqa: E402

from generators._common import load_local_env  # noqa: E402

LIVE_SCHEMAS = ("GOLD", "SNAPSHOTS")


def build() -> bool:
    steps = [
        ("staging views", lambda: dbt(["run", "--select", "path:models/staging", "--target", "ci"])),
        ("snapshots, night by night", lambda: snapshot_nights(["--apply", "--target", "ci"])),
        # An explicit --exclude replaces the default selector, so snapshots must be excluded by name
        # again (Drill 3: they were selected, refused to run without a night, and skipped 20 models).
        ("gold, without unit tests", lambda: dbt(["build", "--exclude", "test_type:unit",
                                                  "resource_type:snapshot",
                                                  "--target", "ci"])),
        ("unit tests", lambda: dbt(["test", "--select", "test_type:unit", "--target", "ci"])),
    ]
    for name, step in steps:
        print(f"\n=== {name}")
        if step() != 0:
            print(f"replay stopped at: {name}")
            return False
    return True


def column_hashes(cur, table: str, cols: list[str]) -> tuple[int, dict[str, int]]:
    parts = ", ".join(f'HASH_AGG("{c}")' for c in cols)
    cur.execute(f"SELECT COUNT(*), {parts} FROM {table}")
    row = cur.fetchone()
    return int(row[0]), dict(zip(cols, row[1:], strict=True))


def compare(schema: str) -> bool:
    conn = connect("LEDGERLINE_CI_RUNNER", as_ci=True)
    same = True
    try:
        cur = conn.cursor()
        cur.execute("USE WAREHOUSE LEDGERLINE_WH")
        cur.execute(
            "SELECT table_schema, table_name FROM LEDGERLINE.INFORMATION_SCHEMA.TABLES "
            f"WHERE table_schema IN {LIVE_SCHEMAS} ORDER BY 1, 2"
        )
        objects = cur.fetchall()
        print(f"\n=== compare: {len(objects)} live objects vs LEDGERLINE_CI.{schema}")
        for live_schema, name in objects:
            cur.execute(
                "SELECT column_name FROM LEDGERLINE.INFORMATION_SCHEMA.COLUMNS "
                f"WHERE table_schema = '{live_schema}' AND table_name = '{name}' ORDER BY ordinal_position"
            )
            cols = [r[0] for r in cur.fetchall()]
            live_n, live_h = column_hashes(cur, f'LEDGERLINE.{live_schema}."{name}"', cols)
            try:
                rep_n, rep_h = column_hashes(cur, f'LEDGERLINE_CI.{schema}."{name}"', cols)
            except Exception as e:  # a missing table or column is the finding
                print(f"  DIFF  {live_schema}.{name}: replay unreadable — {str(e).splitlines()[0][:160]}")
                same = False
                continue
            differ = [c for c in cols if live_h[c] != rep_h[c]]
            if live_n == rep_n and not differ:
                print(f"  SAME  {live_schema}.{name}: {live_n:,} rows, {len(cols)} columns")
            else:
                same = False
                print(f"  DIFF  {live_schema}.{name}: rows live {live_n:,} / replay {rep_n:,}; "
                      f"columns differing: {differ or 'none'}")
    finally:
        conn.close()
    return same


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--schema", required=True, help="CI schema to build into (LEDGERLINE_CI.<schema>)")
    parser.add_argument("--keep", action="store_true", help="do not drop the schema at the end")
    parser.add_argument("--compare-only", action="store_true", help="skip the build; compare what is there")
    parser.add_argument("--build-only", action="store_true", help="build, do not compare (implies --keep)")
    parser.add_argument("--raw", help="read a clone of RAW instead: LEDGERLINE_CI.<schema>.SILVER_CHANGES")
    args = parser.parse_args(argv)
    load_local_env(ROOT / ".env")
    os.environ["DBT_CI_SCHEMA"] = args.schema.upper()
    if args.raw:  # the ci target only; production always reads LEDGERLINE.RAW (_sources.yml)
        os.environ["DBT_RAW_DATABASE"], os.environ["DBT_RAW_SCHEMA"] = "LEDGERLINE_CI", args.raw.upper()

    built = args.compare_only or build()
    if args.build_only:
        return 0 if built else 1
    same = compare(args.schema.upper()) if built else False
    if not args.keep:
        dbt(["run-operation", "drop_ci_schema", "--target", "ci"])
    print("\nreplay EQUALS live" if same else "\nreplay DIFFERS from live (or did not finish)")
    return 0 if same else 1


if __name__ == "__main__":
    sys.exit(main())
