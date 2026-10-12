"""One-off repair: turn the three SCD2 snapshot tables from TRANSIENT into PERMANENT, row for row.

    python snowflake/repairs/2026-10-12_permanent_snapshots.py            # plan: what it would do
    python snowflake/repairs/2026-10-12_permanent_snapshots.py --apply    # do it

incidents.md / decisions.md, 2026-10-12 (Session 16): dbt built every table TRANSIENT (no Fail-safe).
A table's kind cannot be altered, and a transient table cannot be CLONED into a permanent one, so each
snapshot is COPIED: per table, in this order —

  1. CREATE TABLE <t>__S16_COPY AS SELECT * FROM <t>     (a new table: permanent, the schema's default)
  2. compare row count and HASH_AGG(*) — the copy must equal the original exactly, or stop
  3. ALTER TABLE <t> SWAP WITH <t>__S16_COPY             (one step: the names trade places)
  4. check <t> is now kind TABLE (permanent) and still equals the old one
  5. DROP the old transient table (now named <t>__S16_COPY). Recoverable with UNDROP for 1 day
     (its Time Travel), which is the backup if anything looks wrong tomorrow.

Not a replay: replaying nights would re-date every hard delete to today (exp_05). A copy changes
nothing a reader can see. Runs as LEDGERLINE_TRANSFORMER, which owns the snapshot tables. Safe to re-run:
a table already permanent is skipped.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from sf import connect  # noqa: E402

from generators._common import load_local_env  # noqa: E402

SCHEMA = "LEDGERLINE.SNAPSHOTS"
TABLES = ("CUSTOMER_HISTORY", "PRODUCT_HISTORY", "SELLER_HISTORY")


def one(cur, sql: str):
    cur.execute(sql)
    return cur.fetchone()


def kind(cur, table: str) -> str | None:
    cur.execute(f"SHOW TABLES LIKE '{table}' IN SCHEMA {SCHEMA}")
    names = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return rows[0][names.index("kind")] if rows else None


def fingerprint(cur, table: str) -> tuple:
    return tuple(one(cur, f"SELECT COUNT(*), HASH_AGG(*) FROM {SCHEMA}.{table}"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    load_local_env(ROOT / ".env")
    conn = connect("LEDGERLINE_TRANSFORMER")
    try:
        cur = conn.cursor()
        cur.execute("USE WAREHOUSE LEDGERLINE_WH")
        for table in TABLES:
            k = kind(cur, table)
            before = fingerprint(cur, table)
            print(f"{table}: kind {k}, rows {before[0]:,}, hash {before[1]}")
            if k == "TABLE":
                print("  already permanent — nothing to do")
                continue
            if k != "TRANSIENT":
                raise SystemExit(f"  unexpected kind {k!r} — stopping")
            if not args.apply:
                print("  plan: copy -> compare -> swap -> check -> drop the old transient table")
                continue
            copy = f"{table}__S16_COPY"
            cur.execute(f"CREATE TABLE {SCHEMA}.{copy} AS SELECT * FROM {SCHEMA}.{table}")
            copied = fingerprint(cur, copy)
            if copied != before or kind(cur, copy) != "TABLE":
                raise SystemExit(f"  copy differs ({copied}, kind {kind(cur, copy)}); {copy} kept")
            cur.execute(f"ALTER TABLE {SCHEMA}.{table} SWAP WITH {SCHEMA}.{copy}")
            after, k_after = fingerprint(cur, table), kind(cur, table)
            if after != before or k_after != "TABLE":
                raise SystemExit(f"  after swap: {after}, kind {k_after} — old table kept as {copy}")
            cur.execute(f"DROP TABLE {SCHEMA}.{copy}")
            print(f"  done: kind {k_after}, rows {after[0]:,}, hash unchanged; old transient dropped "
                  f"(UNDROP TABLE {SCHEMA}.{copy} within 1 day if needed)")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
