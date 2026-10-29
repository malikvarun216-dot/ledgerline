"""The read-only check every statement passes before it reaches Snowflake (Session 24).

The guarantee is the ROLE: ``LEDGERLINE_COPILOT_SVC`` holds only the two reader roles, which cannot
write, create or see past a mask, whatever SQL the model writes. This check is the second line, and
it exists for a different reason: so a request to change data is *answered* with a refusal the
grader can see, instead of reaching Snowflake and failing as a permission error the model then tries
to "repair".

Allowed: exactly one statement, a query (SELECT / WITH / UNION), reading only LEDGERLINE.GOLD
tables. Refused: anything else, including a second statement after a semicolon.
"""

from __future__ import annotations

import logging

import sqlglot
from sqlglot import exp

from copilot.facts import DATABASE, SCHEMA

# sqlglot warns on every CALL / SHOW it parses as a plain command; the refusal already says so.
logging.getLogger("sqlglot").setLevel(logging.ERROR)

# Nodes that write, define or run something. A query containing any of them anywhere is refused.
_WRITES = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.Command,
    exp.TruncateTable,
    exp.Grant,
    exp.Copy,
)


class Refused(ValueError):
    """The statement is not a single read of Gold. ``str(err)`` says why, in plain words."""


def check(sql: str, semantic_views: frozenset[str] = frozenset()) -> str:
    """Return the statement unchanged if it is a single read of GOLD; raise ``Refused`` otherwise.

    ``semantic_views``: fully qualified semantic views that may also be read, through
    ``SEMANTIC_VIEW(...)``. Cortex Analyst's SQL reads Gold that way (found S24); the copilot passes none.
    """
    try:
        statements = [s for s in sqlglot.parse(sql, read="snowflake") if s is not None]
    except sqlglot.errors.ParseError as err:
        # Not ours to judge: Snowflake's own error message is better input for a repair.
        raise SyntaxError(str(err).splitlines()[0]) from err
    if len(statements) != 1:
        raise Refused(f"one statement only; got {len(statements)}")
    tree = statements[0]
    if not isinstance(tree, exp.Query):
        raise Refused(f"only queries may run; this is {type(tree).__name__.upper()}")
    for node in tree.walk():
        if isinstance(node, _WRITES):
            raise Refused(f"a query may not contain {type(node).__name__.upper()}")
    ctes = {c.alias_or_name.upper() for c in tree.find_all(exp.CTE)}
    for table in tree.find_all(exp.Table):
        name = table.name.upper()
        if not name:  # a table function, e.g. FLATTEN(...)
            continue
        if not table.db and name in ctes:
            continue
        db, catalog = table.db.upper(), table.catalog.upper()
        if f"{catalog}.{db}.{name}" in semantic_views:
            continue
        if (db or SCHEMA) != SCHEMA or (catalog or DATABASE) != DATABASE:
            raise Refused(f"only {DATABASE}.{SCHEMA} may be read; got {table.sql(dialect='snowflake')}")
    return sql
