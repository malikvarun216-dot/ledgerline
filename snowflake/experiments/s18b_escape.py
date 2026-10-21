"""Session 18b, experiment 1: how masked Gold data escapes the mask.

    python snowflake/experiments/s18b_escape.py                                  # S18b, as run
    python snowflake/experiments/s18b_escape.py --schema LEDGERLINE.S19 --db-tagged   # S19

Gold's customer city is masked for readers by a tag on the GOLD schema (09_governance.sql). The
S18 experiment's copies in scratch schema S18 were readable in CLEAR TEXT by the analyst
(incidents.md 2026-10-19 "Masked Gold data escaped the mask"). This measures each way the data can
leave GOLD, as the analyst sees the result:

R1 CTAS by the transformer      -> clear (a role that reads clear text writes clear text)
R2 CLONE by the transformer     -> masked (the clone keeps the column's own 'restricted' tag)
R3 CTAS from that clone         -> clear again
R4 a view over GOLD             -> masked (masks run for the querying role, through any view)
R5 the analyst's own CTAS       -> refused (no CREATE TABLE anywhere)
R6 (S19) CTAS packing the city into a VARIANT -> clear (the mask's signature is STRING)

S19 re-run, after 13_database_tag.sql put PII_CLASS = 'restricted' on the whole database
(decisions.md S19): with --db-tagged, R1 and R3 are predicted MASKED — a copy made anywhere in
LEDGERLINE inherits the database's tag. R6 measures the gap that tag does not close.

Every copy is dropped right after it is measured (finally:), so no clear copy outlives its check.
Predictions were written before the first run; a wrong one does not stop the run — it is the
finding. Runs as LEDGERLINE_TRANSFORMER and LEDGERLINE_ANALYST; writes only in the scratch schema.
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

S = "LEDGERLINE.S18"
GOLD = "LEDGERLINE.GOLD.CUSTOMER_CURRENT"
GOV = "LEDGERLINE.GOVERNANCE.PII_CLASS"
CUSTOMERS = 95_696

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
        print(f"        refused: {str(e).splitlines()[0][:160]}")
        return "refused"


def masked(cur, obj: str) -> int:
    return one(cur, f"SELECT COUNT_IF(customer_city = '***') FROM {S}.{obj}")


def rows(cur, obj: str) -> int:
    return one(cur, f"SELECT COUNT(*) FROM {S}.{obj}")


def main() -> int:
    global S
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default=S, help="scratch schema to copy into (S18b: LEDGERLINE.S18)")
    parser.add_argument("--db-tagged", action="store_true",
                        help="predict for the database-wide 'restricted' tag (S19, 13_database_tag.sql)")
    args = parser.parse_args()
    S = args.schema
    copy_masked = CUSTOMERS if args.db_tagged else 0  # what a plain copy shows the analyst
    load_local_env()
    t_conn, a_conn = connect(role="LEDGERLINE_TRANSFORMER"), connect(role="LEDGERLINE_ANALYST")
    t, a = t_conn.cursor(), a_conn.cursor()
    made: list[str] = []
    try:
        print("\nR1. CTAS by the transformer")
        t.execute(f"CREATE TABLE {S}.ESC_CTAS AS SELECT * FROM {GOLD}")
        made.append("TABLE ESC_CTAS")
        check("R1 analyst rows", rows(a, "ESC_CTAS"), CUSTOMERS)
        check("R1 analyst masked cities", masked(a, "ESC_CTAS"), copy_masked)
        t.execute(f"DROP TABLE {S}.ESC_CTAS")
        made.remove("TABLE ESC_CTAS")

        print("\nR2. CLONE by the transformer")
        t.execute(f"CREATE TABLE {S}.ESC_CLONE CLONE {GOLD}")
        made.append("TABLE ESC_CLONE")
        check("R2 analyst masked cities", masked(a, "ESC_CLONE"), CUSTOMERS)
        tag = "SELECT SYSTEM$GET_TAG('{gov}', '{s}.ESC_CLONE.{col}', 'column')"
        check("R2 clone's city tag", one(t, tag.format(gov=GOV, s=S, col="CUSTOMER_CITY")), "restricted")
        check("R2 clone's state tag", one(t, tag.format(gov=GOV, s=S, col="CUSTOMER_STATE")), "public")
        check("R2 row policy carried to the clone", one(t, f"""SELECT COUNT(*) FROM TABLE(
              LEDGERLINE.INFORMATION_SCHEMA.POLICY_REFERENCES(REF_ENTITY_NAME => '{S}.ESC_CLONE',
              REF_ENTITY_DOMAIN => 'table')) WHERE policy_kind = 'ROW_ACCESS_POLICY'"""), 1)

        print("\nR3. CTAS from the clone, by the transformer")
        t.execute(f"CREATE TABLE {S}.ESC_CTAS2 AS SELECT * FROM {S}.ESC_CLONE")
        made.append("TABLE ESC_CTAS2")
        check("R3 analyst masked cities", masked(a, "ESC_CTAS2"), copy_masked)
        for obj in ("ESC_CTAS2", "ESC_CLONE"):
            t.execute(f"DROP TABLE {S}.{obj}")
            made.remove(f"TABLE {obj}")

        print("\nR4. a view over GOLD, owned by the transformer")
        t.execute(f"CREATE VIEW {S}.ESC_VIEW AS SELECT * FROM {GOLD}")
        made.append("VIEW ESC_VIEW")
        check("R4 analyst masked cities", masked(a, "ESC_VIEW"), CUSTOMERS)
        t.execute(f"DROP VIEW {S}.ESC_VIEW")
        made.remove("VIEW ESC_VIEW")

        print("\nR5. the analyst copies Gold itself")
        check("R5 analyst CTAS", attempt(a, f"CREATE TABLE {S}.ESC_MINE AS SELECT * FROM {GOLD}"), "refused")
    finally:
        for obj in made:  # a failed check must not leave a clear copy behind
            t.execute(f"DROP {obj.split()[0]} IF EXISTS {S}.{obj.split()[1]}")
        t_conn.close()
        a_conn.close()
    wrong = [r for r in results if not r[3]]
    print(f"\n{len(results) - len(wrong)}/{len(results)} as predicted")
    for label, got, predicted, _ in wrong:
        print(f"  WRONG {label}: got {got!r}, predicted {predicted!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
