"""One-off repair (decisions.md, "A deleted dimension row ends on the business date of the dump", Drill 3):
re-date the deletes in the live SCD2 history to the dump that dropped them, as the new snapshot code does.

    python snowflake/repairs/2026-10-30_delete_business_dates.py           # plan: counts only
    python snowflake/repairs/2026-10-30_delete_business_dates.py --apply    # backup clones, then the rewrite

What changes, per history table (customer 250, product 400, seller 400 deleted keys):
  1. a zero-copy backup: SNAPSHOTS.<T>_PRE_DELETE_DATES (kept until the replay comparison is SAME);
  2. new column IS_DELETED, FALSE on every existing row;
  3. each deleted key's closed version: DBT_VALID_TO from the snapshot's run time (2026-10-11 19:45)
     to the business date of the dump that no longer had it (2017-12-28 .. 2018-12-23);
  4. one IS_DELETED row per deleted key, open from that date, shaped exactly as dbt's timestamp
     strategy writes it (DBT_SCD_ID = md5(key || '|' || updated_at), as the live rows are).
Steps 2-4 run in one transaction; any count other than the expected one rolls it all back.

Why a rewrite and not a rebuild: the transformer owns the live history but cannot read LEDGERLINE_CI,
and the CI role cannot write LEDGERLINE. The proof is the comparison afterwards:
`scripts/replay_gold.py --schema DRILL3_DELETES --compare-only` must say SAME for every history table
(the replay was built from empty by the new code, so it is the reference).

Runs as LEDGERLINE_TRANSFORMER (scripts/sf.py).
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

S = "LEDGERLINE.SNAPSHOTS"
TABLES = {  # history table -> (staging view, key, attribute columns)
    "CUSTOMER_HISTORY": ("STG_CUSTOMER", "customer_unique_id",
                         ["customer_zip_code_prefix", "customer_city", "customer_state"]),
    "PRODUCT_HISTORY": ("STG_PRODUCT", "product_id",
                        ["product_category_name", "product_weight_g", "product_length_cm",
                         "product_height_cm", "product_width_cm", "product_photos_qty"]),
    "SELLER_HISTORY": ("STG_SELLER", "seller_id", ["seller_zip_code_prefix", "seller_city", "seller_state"]),
}
EXPECTED = {"CUSTOMER_HISTORY": 250, "PRODUCT_HISTORY": 400, "SELLER_HISTORY": 400}


def deletes_sql(stg: str, key: str, attrs: list[str]) -> str:
    """Each deleted key with its preimage values and the dump date of its delete night (= dims_state)."""
    return f"""
    WITH night_dump AS (
        SELECT night, max(_last_changed_dump_date)::timestamp_ntz AS dump_at
        FROM LEDGERLINE.STAGING.{stg} WHERE change_type <> 'delete' GROUP BY night
    ), newest AS (
        SELECT * FROM LEDGERLINE.STAGING.{stg}
        QUALIFY row_number() OVER (PARTITION BY {key} ORDER BY commit_version DESC) = 1
    )
    SELECT n.{key}, {', '.join('n.' + a for a in attrs)}, d.dump_at, n.commit_version, n.night
    FROM newest n JOIN night_dump d ON d.night = n.night
    WHERE n.change_type = 'delete'"""


def targets_sql(table: str, stg: str, key: str, attrs: list[str]) -> str:
    """Each deleted key's LATEST version, if it is closed (closed by the run-time invalidate): what moves."""
    return f"""
    SELECT l.dbt_scd_id, d.dump_at
    FROM (SELECT {key}, dbt_scd_id, dbt_valid_to FROM {S}.{table}
          QUALIFY row_number() OVER (PARTITION BY {key} ORDER BY dbt_valid_from DESC) = 1) l
    JOIN ({deletes_sql(stg, key, attrs)}) d ON d.{key} = l.{key}
    WHERE l.dbt_valid_to <> '9999-12-31'::timestamp_ntz"""


def one(cur, sql: str):
    cur.execute(sql)
    return cur.fetchone()


def plan(cur) -> bool:
    ok = True
    for table, (stg, key, attrs) in TABLES.items():
        n_del, first, last = one(cur, "SELECT count(*), min(dump_at), max(dump_at) "
                                      f"FROM ({deletes_sql(stg, key, attrs)})")
        closed = one(cur, f"SELECT count(*) FROM ({targets_sql(table, stg, key, attrs)})")[0]
        cur.execute(f"SHOW COLUMNS LIKE 'IS_DELETED' IN TABLE {S}.{table}")
        has_col = bool(cur.fetchall())
        night = one(cur, f"SELECT max(silver_night) FROM {S}.{table}")[0]
        good = n_del == closed == EXPECTED[table] and not has_col and night == 8
        ok &= good
        print(f"  {'ok  ' if good else 'STOP'} {table}: {n_del} deleted keys, {closed} closed latest "
              f"versions (expected {EXPECTED[table]}), "
              f"dump dates {first} .. {last}; already IS_DELETED: {has_col}; holds night {night}")
    return ok


def apply(cur) -> None:
    for table, (stg, key, attrs) in TABLES.items():
        d = deletes_sql(stg, key, attrs)
        cur.execute(f"CREATE TABLE {S}.{table}_PRE_DELETE_DATES CLONE {S}.{table}")
        cur.execute(f"ALTER TABLE {S}.{table} ADD COLUMN IS_DELETED BOOLEAN")  # DDL: commits on its own
        cur.execute("BEGIN")
        try:
            cur.execute(f"UPDATE {S}.{table} SET IS_DELETED = FALSE")
            cur.execute(f"""
                UPDATE {S}.{table} h SET dbt_valid_to = t.dump_at
                FROM ({targets_sql(table, stg, key, attrs)}) t
                WHERE h.dbt_scd_id = t.dbt_scd_id""")
            closed = cur.rowcount
            cur.execute(f"""
                INSERT INTO {S}.{table}
                    ({key}, {', '.join(attrs)}, dim_updated_at, silver_version, silver_night,
                     dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to, is_deleted)
                SELECT {key}, {', '.join(attrs)}, dump_at, commit_version, night,
                       md5(coalesce(cast({key} AS varchar), '') || '|'
                           || coalesce(cast(dump_at AS varchar), '')),
                       dump_at, dump_at, '9999-12-31'::timestamp_ntz, TRUE
                FROM ({d})""")
            inserted = cur.rowcount
            if closed != EXPECTED[table] or inserted != EXPECTED[table]:
                raise RuntimeError(f"{table}: closed {closed}, inserted {inserted}, "
                                   f"expected {EXPECTED[table]} each")
            cur.execute("COMMIT")
            print(f"  {table}: {closed} versions re-dated, {inserted} deleted rows added, committed")
        except Exception:
            cur.execute("ROLLBACK")
            raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="make the backups and rewrite (default: plan)")
    args = parser.parse_args(argv)
    load_local_env(ROOT / ".env")
    conn = connect("LEDGERLINE_TRANSFORMER")
    try:
        cur = conn.cursor()
        cur.execute("USE WAREHOUSE LEDGERLINE_WH")
        print("plan:")
        if not plan(cur):
            print("a precondition failed; nothing written")
            return 1
        if args.apply:
            apply(cur)
        else:
            print("plan only — run again with --apply")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
