"""Session 18 experiment: who sees what on Snowflake, and what a rebuild or a drop does to it.

    python snowflake/experiments/s18_governance.py

Gold's customer columns (zip prefix, city, state) are quasi-identifiers: with a date and an order
they can point at one person. Before policies go on production GOLD, this measures, on a copy of
GOLD.CUSTOMER_CURRENT (95,696 customers, 40,143 in SP), the questions the design depends on:

A. a masking policy attached to a column: the analyst sees '***', the owner clear text; a WHERE
   on the masked column runs on the MASKED value; dbt's CREATE OR REPLACE TABLE drops the policy
   (with and without COPY GRANTS) — the table comes back unmasked
C. tag propagation (PROPAGATE = ON_DEPENDENCY_AND_DATA_MOVEMENT): does a tag on a source column
   reach the column of a table built from it by CTAS, as dbt builds tables?
D. a row access policy driven by a mapping table (role -> state): the analyst sees SP only; it
   applies to the OWNER too (unlike a grant), and through a view; a rebuild drops it as well
E. Time Travel and UNDROP: the table before a DELETE; masking still applies to the past; UNDROP is
   refused while a new table holds the name; retention 0 refuses both
B. tag-based masking: binding a policy to a tag needs APPLY MASKING POLICY on the account, which
   the transformer does not hold — the refusal is the prediction, and it decides who owns policies

Predictions were written before the first run (progress.md S18); a wrong one does not stop the run —
it is the finding. Runs as LEDGERLINE_TRANSFORMER (owner) and LEDGERLINE_ANALYST (reader, a separate
session holding only that role); writes only in LEDGERLINE.S18.
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

S = "LEDGERLINE.S18"
CUSTOMERS = 95_696  # GOLD.CUSTOMER_CURRENT, 2026-10-18
SP = 40_143  # ... in state SP
NOT_SP = CUSTOMERS - SP  # 55,553
TRANSFORMER = "LEDGERLINE_TRANSFORMER"
ANALYST = "LEDGERLINE_ANALYST"

results: list[tuple[str, object, object, bool]] = []


def check(label, got, predicted):
    ok = got == predicted
    results.append((label, got, predicted, ok))
    print(f"  {'ok   ' if ok else 'WRONG'} {label}: got {got!r}, predicted {predicted!r}")


def one(cur, sql: str):
    cur.execute(sql)
    return cur.fetchone()[0]


def attempt(cur, sql: str) -> str:
    """Run a statement; return 'ok' or the error's first line (a refusal is a result here)."""
    try:
        cur.execute(sql)
        return "ok"
    except Exception as e:
        msg = str(e).splitlines()[0]
        print(f"        refused: {msg[:160]}")
        return "refused"


def policies_on(cur, table: str) -> int:
    return one(cur, f"""SELECT COUNT(*) FROM TABLE(LEDGERLINE.INFORMATION_SCHEMA.POLICY_REFERENCES(
                        REF_ENTITY_NAME => '{S}.{table}', REF_ENTITY_DOMAIN => 'table'))""")


def rows(cur, table: str, at: str = "") -> int:
    return one(cur, f"SELECT COUNT(*) FROM {S}.{table} {at}")


def masked(cur, table: str, at: str = "") -> int:
    return one(cur, f"SELECT COUNT_IF(customer_city = '***') FROM {S}.{table} {at}")


def setup(t) -> None:
    print("\nsetup: a copy of GOLD.CUSTOMER_CURRENT, built the way dbt builds a table")
    t.execute(f"CREATE OR REPLACE TABLE {S}.CUST_SRC AS SELECT * FROM LEDGERLINE.GOLD.CUSTOMER_CURRENT")
    t.execute(f"CREATE OR REPLACE TABLE {S}.CUST AS SELECT * FROM {S}.CUST_SRC")
    check("setup: rows copied", rows(t, "CUST"), CUSTOMERS)
    check("setup: SP rows", one(t, f"SELECT COUNT_IF(customer_state = 'SP') FROM {S}.CUST"), SP)


def part_a(t, a) -> None:
    print("\nA. a masking policy on one column, then dbt's CREATE OR REPLACE")
    t.execute(f"""CREATE OR REPLACE MASKING POLICY {S}.MASK_CITY AS (val STRING) RETURNS STRING ->
                  CASE WHEN IS_ROLE_IN_SESSION('{TRANSFORMER}') THEN val ELSE '***' END""")
    check("A1 owner may attach its own policy to its own table",
          attempt(t, f"ALTER TABLE {S}.CUST MODIFY COLUMN customer_city "
                     f"SET MASKING POLICY {S}.MASK_CITY"), "ok")
    check("A2 owner sees clear text (masked rows)", masked(t, "CUST"), 0)
    check("A2 analyst sees '***' on every row", masked(a, "CUST"), CUSTOMERS)
    sao_paulo = one(t, f"SELECT COUNT(*) FROM {S}.CUST WHERE customer_city = 'sao paulo'")
    print(f"        owner: {sao_paulo:,} customers in 'sao paulo'")
    check("A3 analyst WHERE city = 'sao paulo' runs on the masked value",
          one(a, f"SELECT COUNT(*) FROM {S}.CUST WHERE customer_city = 'sao paulo'"), 0)
    t.execute(f"CREATE OR REPLACE TABLE {S}.CUST AS SELECT * FROM {S}.CUST_SRC")
    check("A4 after CREATE OR REPLACE: policies on the table", policies_on(t, "CUST"), 0)
    check("A4 after CREATE OR REPLACE: analyst masked rows", masked(a, "CUST"), 0)
    t.execute(f"ALTER TABLE {S}.CUST MODIFY COLUMN customer_city SET MASKING POLICY {S}.MASK_CITY")
    t.execute(f"CREATE OR REPLACE TABLE {S}.CUST COPY GRANTS AS SELECT * FROM {S}.CUST_SRC")
    check("A5 after CREATE OR REPLACE ... COPY GRANTS: policies", policies_on(t, "CUST"), 0)


def part_c(t, a) -> None:
    print("\nC. tag propagation: does a source column's tag reach a table built from it?")
    created = attempt(t, f"""CREATE OR REPLACE TAG {S}.PII_PROP ALLOWED_VALUES 'quasi_identifier'
                             PROPAGATE = ON_DEPENDENCY_AND_DATA_MOVEMENT""")
    check("C1 a propagating tag is accepted (trial, Enterprise, 10.36)", created, "ok")
    if created != "ok":
        return
    t.execute(f"ALTER TABLE {S}.CUST_SRC MODIFY COLUMN customer_city "
              f"SET TAG {S}.PII_PROP = 'quasi_identifier'")
    t.execute(f"CREATE OR REPLACE TABLE {S}.CUST_P AS SELECT * FROM {S}.CUST_SRC")
    found = 0
    for _ in range(6):  # propagation may lag the CTAS; give it up to a minute
        found = one(t, f"""SELECT COUNT(*) FROM TABLE(LEDGERLINE.INFORMATION_SCHEMA.TAG_REFERENCES(
                           '{S}.CUST_P.CUSTOMER_CITY', 'column')) WHERE tag_name = 'PII_PROP'""")
        if found:
            break
        time.sleep(10)
    check("C2 the CTAS target column carries the tag", found, 1)


def part_d(t, a) -> None:
    print("\nD. a row access policy from a mapping table (role -> state)")
    t.execute(f"CREATE OR REPLACE TABLE {S}.STATE_ACCESS (role_name STRING, customer_state STRING)")
    t.execute(f"INSERT INTO {S}.STATE_ACCESS VALUES ('{ANALYST}', 'SP')")
    t.execute(f"""CREATE OR REPLACE ROW ACCESS POLICY {S}.STATE_ROWS AS (state STRING) RETURNS BOOLEAN ->
                  EXISTS (SELECT 1 FROM {S}.STATE_ACCESS m
                          WHERE m.customer_state IN (state, '*') AND IS_ROLE_IN_SESSION(m.role_name))""")
    t.execute(f"CREATE OR REPLACE TABLE {S}.CUST_R AS SELECT * FROM {S}.CUST_SRC")
    check("D1 owner may attach its own row policy",
          attempt(t, f"ALTER TABLE {S}.CUST_R ADD ROW ACCESS POLICY {S}.STATE_ROWS "
                     "ON (customer_state)"), "ok")
    check("D2 analyst sees SP only", rows(a, "CUST_R"), SP)
    # The owner is not mapped. It still holds ANALYST below it in the hierarchy, so it sees SP too:
    # a row policy applies to the owner, and IS_ROLE_IN_SESSION counts inherited roles.
    check("D3 the owner is filtered too (by its inherited ANALYST)", rows(t, "CUST_R"), SP)
    t.execute(f"INSERT INTO {S}.STATE_ACCESS VALUES ('{TRANSFORMER}', '*')")
    check("D4 owner mapped to '*' sees every row", rows(t, "CUST_R"), CUSTOMERS)
    t.execute(f"CREATE OR REPLACE VIEW {S}.CUST_R_V AS SELECT * FROM {S}.CUST_R")
    check("D5 analyst through the owner's view: still SP only", rows(a, "CUST_R_V"), SP)
    t.execute(f"CREATE OR REPLACE TABLE {S}.CUST_R AS SELECT * FROM {S}.CUST_SRC")
    check("D6 after CREATE OR REPLACE: analyst sees every row", rows(a, "CUST_R"), CUSTOMERS)


def part_e(t, a) -> None:
    print("\nE. Time Travel and UNDROP")
    t.execute(f"CREATE OR REPLACE TABLE {S}.TT AS SELECT * FROM {S}.CUST_SRC")
    t.execute(f"ALTER TABLE {S}.TT MODIFY COLUMN customer_city SET MASKING POLICY {S}.MASK_CITY")
    t.execute(f"DELETE FROM {S}.TT WHERE customer_state = 'SP'")
    qid = t.sfqid
    check("E1 rows now", rows(t, "TT"), NOT_SP)
    check("E1 rows BEFORE the delete", rows(t, "TT", f"BEFORE(STATEMENT => '{qid}')"), CUSTOMERS)
    check("E2 analyst's time travel is still masked",
          masked(a, "TT", f"BEFORE(STATEMENT => '{qid}')"), CUSTOMERS)
    t.execute(f"DROP TABLE {S}.TT")
    t.execute(f"CREATE TABLE {S}.TT (x INT)")
    check("E3 UNDROP while a new table holds the name", attempt(t, f"UNDROP TABLE {S}.TT"), "refused")
    t.execute(f"ALTER TABLE {S}.TT RENAME TO {S}.TT_NEW")
    check("E3 UNDROP after the rename", attempt(t, f"UNDROP TABLE {S}.TT"), "ok")
    check("E3 undropped rows = rows at the drop", rows(t, "TT"), NOT_SP)
    check("E4 the masking policy came back with it", policies_on(t, "TT"), 1)
    t.execute(f"CREATE OR REPLACE TABLE {S}.TT0 AS SELECT * FROM {S}.CUST_SRC")
    t.execute(f"ALTER TABLE {S}.TT0 SET DATA_RETENTION_TIME_IN_DAYS = 0")
    t.execute(f"DELETE FROM {S}.TT0 WHERE customer_state = 'SP'")
    qid0 = t.sfqid
    check("E5 retention 0: time travel refused",
          attempt(t, f"SELECT COUNT(*) FROM {S}.TT0 BEFORE(STATEMENT => '{qid0}')"), "refused")
    t.execute(f"DROP TABLE {S}.TT0")
    check("E5 retention 0: UNDROP refused", attempt(t, f"UNDROP TABLE {S}.TT0"), "refused")


def part_b(t, a) -> None:
    print("\nB. tag-based masking: bind a policy to a tag")
    t.execute(f"CREATE OR REPLACE TAG {S}.PII_CLASS ALLOWED_VALUES 'quasi_identifier', 'public'")
    check("B1 the transformer binds a policy to its own tag (no APPLY MASKING POLICY on account)",
          attempt(t, f"ALTER TAG {S}.PII_CLASS SET MASKING POLICY {S}.MASK_CITY"), "refused")


def main() -> int:
    load_local_env()
    t_conn = connect(role=TRANSFORMER)
    a_conn = connect(role=ANALYST)
    t, a = t_conn.cursor(), a_conn.cursor()
    print("transformer session:", one(t, "SELECT CURRENT_ROLE() || ' / ' || CURRENT_SECONDARY_ROLES()"))
    print("analyst session:    ", one(a, "SELECT CURRENT_ROLE() || ' / ' || CURRENT_SECONDARY_ROLES()"))
    try:
        setup(t)
        part_a(t, a)
        part_c(t, a)
        part_d(t, a)
        part_e(t, a)
        part_b(t, a)
    finally:
        t_conn.close()
        a_conn.close()
    wrong = [r for r in results if not r[3]]
    print(f"\n{len(results) - len(wrong)}/{len(results)} as predicted")
    for label, got, predicted, _ in wrong:
        print(f"  WRONG {label}: got {got!r}, predicted {predicted!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
