"""Compare Gold on Snowflake with Gold on Databricks, row for row (Session 28).

    python scripts/diff_gold.py                    # every Gold model and snapshot
    python scripts/diff_gold.py fct_orders dim_customer
    python scripts/diff_gold.py --show 5           # print up to 5 differing rows per table

Why (decisions.md S28): a second dbt target is a fallback only if it builds the SAME Gold. Both
Golds start from the same bridge files, so every difference is dbt code or platform behaviour —
and this finds it by row, not by a total.

How: each table is read whole from both sides (the largest is ~113k rows), every value is put in
one plain form (text, money at 2 places, floats at 6, timestamps without a zone, NULL as None), and
the rows are compared as multisets. A table "matches" when both sides hold exactly the same rows.

Not compared, by name (PLATFORM_LOCAL): values that are by design each platform's own —
  raw_loaded_at  when that platform's RAW received the row (Snowpipe vs COPY INTO)
  dbt_scd_id     a hash dbt makes from the key and a timestamp's TEXT, which each platform prints
                 differently; its partner dbt_valid_from is compared
Snowflake is read as LEDGERLINE_TRANSFORMER (row policy '*', masks exempt — clear values, as
Databricks Gold has no masks); Databricks as the `ledgerline-ci` service principal.
"""

from __future__ import annotations

import argparse
import datetime as dt
import decimal
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from dbx import cursor as databricks_cursor  # noqa: E402
from sf import connect  # noqa: E402

from generators._common import load_local_env  # noqa: E402

GOLD = ["customer_current", "product_current", "seller_current", "inventory_current",
        "dim_customer", "dim_product", "dim_seller", "fct_orders", "fct_order_items", "fct_reviews",
        "order_items_at_sale", "review_labels", "delivery_promise", "seller_scorecard",
        "late_vs_rating", "complaint_trends"]
SNAPSHOTS = ["customer_history", "product_history", "seller_history"]
PLATFORM_LOCAL = {"raw_loaded_at", "dbt_scd_id"}


def norm(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float, decimal.Decimal)):
        # One form for every number, whichever type each platform returned (NUMBER(…, 4) arrives as
        # a Decimal from Snowflake, the same value as a float from Databricks): 6 places, no
        # trailing zeros. "12.50" and 12.5 and 12.500000 are one value.
        d = decimal.Decimal(repr(v) if isinstance(v, float) else v).quantize(decimal.Decimal("0.000001"))
        text = f"{d:f}"
        return text.rstrip("0").rstrip(".") if "." in text else text
    if isinstance(v, dt.datetime):
        if v.tzinfo is not None:
            v = v.astimezone(dt.UTC).replace(tzinfo=None)
        return v.isoformat(sep=" ")
    if isinstance(v, dt.date):
        return v.isoformat()
    return str(v)


def read_snowflake(cur, schema, table):
    cur.execute(f"SELECT * FROM LEDGERLINE.{schema}.{table}")
    cols = [d[0].lower() for d in cur.description]
    return cols, cur.fetchall()


def read_databricks(cur, schema, table):
    cur.execute(f"SELECT * FROM workspace.{schema}.{table}")
    cols = [d[0].lower() for d in cur.description]
    return cols, [tuple(r) for r in cur.fetchall()]


def compare(name, sf_side, dbx_side, show):
    (sf_cols, sf_rows), (dbx_cols, dbx_rows) = sf_side, dbx_side
    common = [c for c in sf_cols if c in dbx_cols and c not in PLATFORM_LOCAL]
    only_sf = [c for c in sf_cols if c not in dbx_cols]
    only_dbx = [c for c in dbx_cols if c not in sf_cols]
    si = [sf_cols.index(c) for c in common]
    di = [dbx_cols.index(c) for c in common]
    a = Counter(tuple(norm(r[i]) for i in si) for r in sf_rows)
    b = Counter(tuple(norm(r[i]) for i in di) for r in dbx_rows)
    missing, extra = a - b, b - a
    ok = not missing and not extra and not only_sf and not only_dbx
    print(f"{'MATCH' if ok else 'DIFF '} {name:<22} snowflake {len(sf_rows):>7}"
          f"  databricks {len(dbx_rows):>7}"
          f"  only-sf {sum(missing.values()):>6}  only-dbx {sum(extra.values()):>6}")
    if only_sf or only_dbx:
        print(f"      columns only on snowflake {only_sf}, only on databricks {only_dbx}")
    if (missing or extra) and show:
        # Which columns differ: compare the rows pairwise by their first column (the key, usually).
        for label, side in (("snowflake", missing), ("databricks", extra)):
            for row in list(side)[:show]:
                print(f"      only {label}: {dict(zip(common, row, strict=True))}")
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("tables", nargs="*")
    parser.add_argument("--show", type=int, default=0)
    args = parser.parse_args()
    load_local_env()
    todo = [("gold", t) for t in GOLD] + [("snapshots", t) for t in SNAPSHOTS]
    if args.tables:
        todo = [(s, t) for s, t in todo if t in args.tables]

    sf = connect(role="LEDGERLINE_TRANSFORMER").cursor()
    sf.execute("USE WAREHOUSE LEDGERLINE_WH")
    dbx = databricks_cursor()
    results = []
    for schema, table in todo:
        try:
            results.append(compare(table, read_snowflake(sf, schema.upper(), table.upper()),
                                   read_databricks(dbx, schema, table), args.show))
        except Exception as error:
            print(f"ERROR {table:<22} {str(error).splitlines()[0][:200]}")
            results.append(False)
    print(f"\n{sum(results)}/{len(results)} tables identical")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
