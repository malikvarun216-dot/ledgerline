"""Session 19, experiment 3: Snowpark, small — a Python UDF and a Python stored procedure.

    python snowflake/experiments/s19_snowpark.py

Snowpark = Python (or Java / Scala) that runs INSIDE Snowflake, next to the data, instead of pulling
rows to a laptop. Two shapes:
  UDF        a function called per row from SQL. Runs in a Python sandbox on the warehouse.
  PROCEDURE  a program called once (CALL); it gets a Snowpark Session and can run SQL, loop, branch.

G  a Python UDF re-pads zips the way Silver does (Bronze reads them as numbers: 09790 -> 9790,
   constraints.md "our data"). Equal to SQL's LPAD on all 96,358 staged customer rows. The point
   to measure is that it is not needed: a built-in does the same with no sandbox.
H  a Python procedure SUSPEND_ALL_TASKS(schema): the cost guard every Snowflake session with tasks
   needs (coverage S19: "every Task suspended at the end"), written once, called by name. Two
   dummy tasks resumed, one CALL, both suspended. It needs the Snowpark package
   (PACKAGES = ('snowflake-snowpark-python')) — a trial may require the Anaconda terms first.

Predictions were written before the first run; a wrong one does not stop the run — it is the
finding. Every task suspended in finally:. Runs as LEDGERLINE_TRANSFORMER; writes only in S19.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from sf import connect  # noqa: E402

from generators._common import load_local_env  # noqa: E402

S = "LEDGERLINE.S19"
CUSTOMER_ROWS = 96_358  # STAGING.STG_CUSTOMER: every customer change Silver exported

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
        print(f"        refused: {' '.join(str(e).split())[:300]}")
        return "refused"


PAD_ZIP = f"""CREATE OR REPLACE FUNCTION {S}.PAD_ZIP(z NUMBER)
RETURNS STRING LANGUAGE PYTHON RUNTIME_VERSION = '3.11' HANDLER = 'pad'
AS $$
def pad(z):
    return None if z is None else str(int(z)).zfill(5)
$$"""

SUSPEND_ALL = f"""CREATE OR REPLACE PROCEDURE {S}.SUSPEND_ALL_TASKS(schema_name STRING)
RETURNS STRING LANGUAGE PYTHON RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python') HANDLER = 'run' EXECUTE AS CALLER
AS $$
def run(session, schema_name):
    tasks = session.sql(f"SHOW TASKS IN SCHEMA {{schema_name}}").collect()
    started = [t["name"] for t in tasks if t["state"] == "started"]
    # roots first: a child task cannot be altered (suspend included) while its root is started.
    # SHOW lists predecessors as a JSON array, empty for a root
    roots = {{t["name"] for t in tasks if t["predecessors"] in ("[]", "", None)}}
    started.sort(key=lambda n: n not in roots)
    for name in started:
        session.sql(f"ALTER TASK {{schema_name}}.{{name}} SUSPEND").collect()
    return f"suspended {{len(started)}} of {{len(tasks)}}: {{', '.join(started)}}"
$$"""


def main() -> int:
    load_local_env()
    conn = connect(role="LEDGERLINE_TRANSFORMER")
    c = conn.cursor()
    try:
        c.execute("USE WAREHOUSE LEDGERLINE_WH")
        print("\nG. a Python UDF beside a built-in")
        check("G1 create the UDF", attempt(c, PAD_ZIP), "ok")
        check(
            "G2 UDF(zip as a number) = the staged zip, every row",
            one(
                c,
                f"""SELECT COUNT_IF({S}.PAD_ZIP(customer_zip_code_prefix::NUMBER) = customer_zip_code_prefix)
                FROM LEDGERLINE.STAGING.STG_CUSTOMER""",
            ),
            CUSTOMER_ROWS,
        )
        check(
            "G3 the built-in LPAD does the same",
            one(
                c,
                """SELECT COUNT_IF(LPAD(customer_zip_code_prefix::NUMBER::STRING, 5, '0')
                                = customer_zip_code_prefix)
                FROM LEDGERLINE.STAGING.STG_CUSTOMER""",
            ),
            CUSTOMER_ROWS,
        )

        print("\nH. a Python procedure: suspend every task in a schema")
        made = attempt(c, SUSPEND_ALL)
        check("H1 create the procedure (Snowpark package)", made, "ok")
        if made == "ok":
            c.execute(
                f"CREATE OR REPLACE TASK {S}.PING_ROOT WAREHOUSE = LEDGERLINE_WH SCHEDULE = '60 MINUTE' "
                "AS SELECT 1"
            )
            c.execute(
                f"CREATE OR REPLACE TASK {S}.PING_CHILD WAREHOUSE = LEDGERLINE_WH AFTER {S}.PING_ROOT "
                "AS SELECT 1"
            )
            c.execute(f"ALTER TASK {S}.PING_CHILD RESUME")
            c.execute(f"ALTER TASK {S}.PING_ROOT RESUME")
            c.execute(f"SHOW TASKS IN SCHEMA {S}")
            cols = [d[0].lower() for d in c.description]
            check(
                "H2 started before the CALL",
                sum(r[cols.index("state")] == "started" for r in c.fetchall()),
                2,
            )
            out = one(c, f"CALL {S}.SUSPEND_ALL_TASKS('{S}')")
            print(f"        {out}")
            check("H3 the CALL suspended both", out.startswith("suspended 2 of"), True)
    finally:
        c.execute(f"SHOW TASKS IN SCHEMA {S}")
        cols = [d[0].lower() for d in c.description]
        started = [r for r in c.fetchall() if r[cols.index("state")] == "started"]
        started.sort(key=lambda r: r[cols.index("predecessors")] not in ("[]", "", None))  # roots first
        for r in started:
            c.execute(f"ALTER TASK {S}.{r[cols.index('name')]} SUSPEND")
        c.execute(f"SHOW TASKS IN SCHEMA {S}")
        states = {r[cols.index("name")]: r[cols.index("state")] for r in c.fetchall()}
        print(f"\n  tasks after the run: {states}")
        check("every task suspended", all(v == "suspended" for v in states.values()), True)
        conn.close()
    wrong = [r for r in results if not r[3]]
    print(f"\n{len(results) - len(wrong)}/{len(results)} as predicted")
    for label, got, predicted, _ in wrong:
        print(f"  WRONG {label}: got {got!r}, predicted {predicted!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
