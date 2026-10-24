"""Run SQL on Snowflake as the `LEDGERLINE_SVC` service user, and print what comes back.

    python scripts/sf.py "SELECT CURRENT_USER(), CURRENT_ROLE()"
    python scripts/sf.py --role LEDGERLINE_TRANSFORMER "SELECT COUNT(*) FROM LEDGERLINE.RAW.X"
    python scripts/sf.py --file snowflake/10_bridge_load.sql

Why this exists (Session 13): the Snowsight worksheet shows only the last statement's result,
and its "run" key runs only the statement under the cursor (the first cost-guard run did exactly
that). SQL kept in git and run by a script is repeatable, and its output can be read whole.

Signs in with the key pair from ``scripts/sf_keypair.py`` — never a password, never the human's
own user (which is ACCOUNTADMIN and would pass every grant, like the Databricks owner in S12).
Each statement's result is printed; an error is printed and stops the run, with the statement.

Reads from ``.env``: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_PATH, and optionally
SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from generators._common import load_local_env

MAX_ROWS = 50
WIDTH = 110  # widest cell printed; longer values are cut


def connect(role: str | None = None, as_ci: bool = False, as_bi: bool = False):
    """Sign in as the laptop's service user — or, with ``as_ci``, as the CI user (Session 16), whose
    name and key come from SNOWFLAKE_CI_USER / SNOWFLAKE_CI_PRIVATE_KEY_PATH; or, with ``as_bi``, as
    the Power BI reader (Session 23), SNOWFLAKE_BI_USER / SNOWFLAKE_BI_PRIVATE_KEY_PATH, on the
    user's own default warehouse and role — what the report itself gets."""
    import snowflake.connector

    user_var, key_var = (
        ("SNOWFLAKE_CI_USER", "SNOWFLAKE_CI_PRIVATE_KEY_PATH") if as_ci
        else ("SNOWFLAKE_BI_USER", "SNOWFLAKE_BI_PRIVATE_KEY_PATH") if as_bi
        else ("SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_PATH")
    )
    required = ("SNOWFLAKE_ACCOUNT", user_var, key_var)
    missing = [n for n in required if not os.environ.get(n)]
    if missing:
        raise SystemExit(f"missing in .env: {', '.join(missing)}")
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ[user_var],
        authenticator="SNOWFLAKE_JWT",
        private_key_file=os.path.expanduser(os.environ[key_var]),
        role=role or (None if as_bi else os.environ.get("SNOWFLAKE_ROLE")) or None,
        warehouse=(None if as_bi else os.environ.get("SNOWFLAKE_WAREHOUSE")) or None,
        database=os.environ.get("SNOWFLAKE_DATABASE") or None,
        session_parameters={"QUERY_TAG": "ledgerline:scripts/sf.py"},
    )


def show(cursor, statement: str) -> None:
    print(f"\n>>> {' '.join(statement.split())[:160]}")
    if cursor.description is None:
        print("(no result set)")
        return
    names = [d[0] for d in cursor.description]
    rows = cursor.fetchmany(MAX_ROWS + 1)
    more = len(rows) > MAX_ROWS  # rowcount is 'rows affected' for DML, so not used here
    rows = rows[:MAX_ROWS]
    cells = [[("NULL" if v is None else str(v)) for v in row] for row in rows]
    widths = [min(WIDTH, max([len(n)] + [len(r[i]) for r in cells])) for i, n in enumerate(names)]
    print(" | ".join(n.ljust(w) for n, w in zip(names, widths, strict=True)))
    print("-+-".join("-" * w for w in widths))
    for r in cells:
        print(" | ".join(c[:WIDTH].ljust(w) for c, w in zip(r, widths, strict=True)))
    print(f"({len(rows)} row{'s' if len(rows) != 1 else ''}{', more not shown' if more else ''})")


def run(sql: str, role: str | None = None, as_bi: bool = False) -> int:
    import io

    from snowflake.connector.errors import ProgrammingError
    from snowflake.connector.util_text import split_statements

    conn = connect(role, as_bi=as_bi)
    try:
        cur = conn.cursor()
        for statement, _ in split_statements(io.StringIO(sql), remove_comments=True):
            if not statement.strip():
                continue
            try:
                cur.execute(statement)
            except ProgrammingError as err:
                print(f"\n>>> {' '.join(statement.split())[:160]}")
                print(f"ERROR {err.errno} ({err.sqlstate}): {err.msg}")
                return 1
            show(cur, statement)
    finally:
        conn.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("sql", nargs="?", help="SQL text (one or more statements)")
    parser.add_argument("--file", type=Path, help="a .sql file to run top to bottom")
    parser.add_argument("--role", help="role to run as (default: SNOWFLAKE_ROLE, else the user's default)")
    parser.add_argument("--as-bi", action="store_true", help="sign in as the Power BI reader user (S23)")
    args = parser.parse_args(argv)
    if bool(args.sql) == bool(args.file):
        parser.error("give SQL text or --file, not both")
    load_local_env()
    sql = args.file.read_text(encoding="utf-8") if args.file else args.sql
    return run(sql, args.role, as_bi=args.as_bi)


if __name__ == "__main__":
    sys.exit(main())
