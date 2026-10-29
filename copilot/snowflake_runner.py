"""Run statements on Snowflake as the copilot's reader (Session 24).

One connection per eval run, signed in as ``LEDGERLINE_COPILOT_SVC`` (``sf.connect(as_copilot=True)``)
under the role asked for — ``LEDGERLINE_ANALYST`` by default, ``LEDGERLINE_ANALYST_SP`` for the
São Paulo reader. The user holds nothing else, so nothing written here can widen what it sees.

Each statement is tagged and capped: ``QUERY_TAG`` lets ``QUERY_HISTORY`` show every query the
copilot ran; a 60 s statement timeout is tighter than the warehouse's 5 minutes.
"""

from __future__ import annotations

from copilot.facts import DATABASE, SCHEMA, TABLES
from copilot.graph import SqlError

MAX_ROWS = 200  # rows fetched; an answer needing more is the wrong query


class SnowflakeRunner:
    def __init__(self, role: str = "LEDGERLINE_ANALYST", tag: str = "copilot"):
        from scripts import sf

        self.role = role
        self.conn = sf.connect(role, as_copilot=True)
        cur = self.conn.cursor()
        cur.execute(f"ALTER SESSION SET QUERY_TAG = 'ledgerline:{tag}', STATEMENT_TIMEOUT_IN_SECONDS = 60")
        cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
        self.identity = cur.fetchone()

    def run(self, sql: str):
        from snowflake.connector.errors import Error

        cur = self.conn.cursor()
        try:
            cur.execute(sql)
        except Error as err:
            raise SqlError(f"{getattr(err, 'errno', '')} {getattr(err, 'msg', err)}".strip()) from err
        columns = [d[0] for d in cur.description or []]
        return columns, [list(r) for r in cur.fetchmany(MAX_ROWS)], cur.sfqid

    def columns(self) -> dict[str, list[tuple[str, str]]]:
        """The Gold columns THIS role can see (INFORMATION_SCHEMA lists only granted objects)."""
        names = ", ".join(f"'{t}'" for t in TABLES)
        cur = self.conn.cursor()
        cur.execute(
            f"SELECT table_name, column_name, data_type FROM {DATABASE}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE table_schema = '{SCHEMA}' AND table_name IN ({names}) "
            "ORDER BY table_name, ordinal_position"
        )
        out: dict[str, list[tuple[str, str]]] = {}
        for table, column, dtype in cur.fetchall():
            out.setdefault(table, []).append((column, dtype))
        return out

    def close(self) -> None:
        self.conn.close()
