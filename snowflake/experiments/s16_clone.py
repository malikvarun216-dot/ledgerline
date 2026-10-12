"""Session 16 experiment: Snowflake's zero-copy clone — what it shares, what it owns, how it publishes.

    python snowflake/experiments/s16_clone.py

A clone is a new table (or schema, or database) made from another's state without copying data:
Snowflake points the new object at the same micro-partitions (its immutable storage files) and only
writes new ones when either side changes. The Delta half is `experiments/delta_clone.py`; this one
asks the same questions, plus the three slim CI and write-audit-publish depend on.

A. A TABLE clone, as the transformer in LEDGERLINE.S16, from a real copy of GOLD.FCT_ORDERS:
   A1 clone: same rows; it owns no bytes yet (storage metrics); SHOW TABLES still shows full size
   A2 update 100 rows of the clone: the source does not move
   A3 clone a PAST state: `CLONE … BEFORE (STATEMENT => <the update>)`, Time Travel as a table
   A4 DROP the source: the clone still reads every row — unlike Delta's shallow clone (case F)

B. As the CI role in LEDGERLINE_CI (what slim CI and the publish step will do):
   B1 clone production's GOLD schema whole: who owns the cloned tables? Do the source schema's
      future grants (the analyst's read) come along? Managed access?
   B2 replace a table in that cloned schema (what a CI dbt build does to a model)
   B3 clone single TABLES instead (what `dbt clone` does): owner = the role that cloned
   B4 write-audit-publish: an audit schema cloned from a "published" one; a bad write fails the
      audit and nothing is published; a good one passes and `ALTER SCHEMA … SWAP WITH` publishes
      it in one step — the old state left behind under the audit name, ready to swap back

Production is only read. Part A writes in LEDGERLINE.S16; part B in LEDGERLINE_CI and drops its
own schemas at the end. Predictions were written before the first run; a wrong one does not stop
the run — it is the finding (Session 11's rule).
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

S = "LEDGERLINE.S16"
PROD = "LEDGERLINE.GOLD.FCT_ORDERS"
CI = "LEDGERLINE_CI"
B_SCHEMAS = ("S16_GOLD", "S16_TABLES", "S16_PUBLISHED", "S16_AUDIT")

results: list[tuple[str, object, object, bool]] = []


def check(label, got, predicted):
    ok = got == predicted
    results.append((label, got, predicted, ok))
    print(f"  {'ok   ' if ok else 'WRONG'} {label}: got {got!r}, predicted {predicted!r}")


def one(cur, sql: str):
    cur.execute(sql)
    return cur.fetchone()


def attempt(cur, sql: str) -> str:
    """Run a statement; 'ok' or the first line of its error."""
    try:
        cur.execute(sql)
        return "ok"
    except Exception as e:
        return str(e).strip().splitlines()[0][:200]


def same_rows(cur, a: str, b: str) -> bool:
    extra = one(cur, f"SELECT COUNT(*) FROM (SELECT * FROM {a} MINUS SELECT * FROM {b})")[0]
    missing = one(cur, f"SELECT COUNT(*) FROM (SELECT * FROM {b} MINUS SELECT * FROM {a})")[0]
    return extra == 0 and missing == 0


def shown(cur, sql: str, column: str) -> dict:
    """A SHOW command's rows as {name: value of one column}."""
    cur.execute(sql)
    names = [d[0] for d in cur.description]
    return {row[names.index("name")]: row[names.index(column)] for row in cur.fetchall()}


# ---------------------------------------------------------------------------------------------
# A. Table clones, as the transformer
# ---------------------------------------------------------------------------------------------


def part_a(cur) -> None:
    print("\nA. table clones in LEDGERLINE.S16")
    src, clone, past = f"{S}.ORDERS_SRC", f"{S}.ORDERS_CLONE", f"{S}.ORDERS_BEFORE"
    cur.execute(f"CREATE OR REPLACE TABLE {src} AS SELECT * FROM {PROD}")  # a real copy: real bytes
    rows = one(cur, f"SELECT COUNT(*) FROM {src}")[0]
    print(f"  source {src}: {rows:,} rows")

    began = time.time()
    cur.execute(f"CREATE OR REPLACE TABLE {clone} CLONE {src}")
    print(f"  A1 clone took {time.time() - began:.1f} s")
    check("A1 clone holds the source's rows exactly", same_rows(cur, clone, src), True)
    sizes = shown(cur, f"SHOW TABLES LIKE 'ORDERS_%' IN SCHEMA {S}", "bytes")
    # PREDICTION: SHOW TABLES reports what each table READS, so the clone shows the source's size.
    check("A1 SHOW TABLES bytes: clone = source", sizes.get("ORDERS_CLONE") == sizes.get("ORDERS_SRC"), True)
    cur.execute("""SELECT table_name, id, clone_group_id, active_bytes
        FROM LEDGERLINE.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
        WHERE table_schema = 'S16' AND table_name IN ('ORDERS_SRC', 'ORDERS_CLONE')
          AND table_dropped IS NULL""")
    metrics = {r[0]: r[1:] for r in cur.fetchall()}
    print(f"  A1 storage metrics (id, clone_group_id, active_bytes): {metrics or 'none yet'}")
    if not metrics:
        # Session 16, first run: the view had no row for a table minutes old — storage metrics are
        # computed in the background. The byte checks are skipped, not guessed; re-read later.
        print("  A1 (storage metrics lag behind new tables: byte checks skipped this run)")
    if {"ORDERS_SRC", "ORDERS_CLONE"} <= metrics.keys():
        # PREDICTION: the clone joins the source's clone group and OWNS no bytes — it only points.
        check("A1 clone is in the source's clone group",
              metrics["ORDERS_CLONE"][1] == metrics["ORDERS_SRC"][0], True)
        check("A1 clone owns 0 active bytes", metrics["ORDERS_CLONE"][2], 0)

    cur.execute(f"""UPDATE {clone} SET status = 's16_test'
        WHERE order_id IN (SELECT order_id FROM {clone} ORDER BY order_id LIMIT 100)""")
    update_qid = cur.sfqid
    check("A2 rows changed in the clone",
          one(cur, f"SELECT COUNT(*) FROM {clone} WHERE status = 's16_test'")[0], 100)
    check("A2 rows changed in the source",
          one(cur, f"SELECT COUNT(*) FROM {src} WHERE status = 's16_test'")[0], 0)

    cur.execute(f"CREATE OR REPLACE TABLE {past} CLONE {clone} BEFORE (STATEMENT => '{update_qid}')")
    check("A3 clone BEFORE the update: changed rows",
          one(cur, f"SELECT COUNT(*) FROM {past} WHERE status = 's16_test'")[0], 0)
    check("A3 clone BEFORE the update = the source", same_rows(cur, past, src), True)

    cur.execute(f"DROP TABLE {src}")
    # PREDICTION: a Snowflake clone does not depend on its source's lifecycle — partitions are shared
    # and kept while any table uses them. (Delta's shallow clone: experiments/delta_clone.py case F.)
    check("A4 source dropped: the clone still reads every row",
          one(cur, f"SELECT COUNT(*) FROM {clone}")[0], rows)
    check("A4 source dropped: the BEFORE clone still reads every row",
          one(cur, f"SELECT COUNT(*) FROM {past}")[0], rows)


# ---------------------------------------------------------------------------------------------
# B. As the CI role: schema clone vs table clones, and write-audit-publish
# ---------------------------------------------------------------------------------------------


def part_b(cur) -> None:
    print(f"\nB. as {one(cur, 'SELECT CURRENT_ROLE()')[0]} in {CI}")
    for s in B_SCHEMAS:
        cur.execute(f"DROP SCHEMA IF EXISTS {CI}.{s}")

    b1 = attempt(cur, f"CREATE SCHEMA {CI}.S16_GOLD CLONE LEDGERLINE.GOLD")
    print(f"  B1 clone the GOLD schema whole: {b1}")
    if b1 == "ok":
        owners = shown(cur, f"SHOW TABLES IN SCHEMA {CI}.S16_GOLD", "owner")
        print(f"  B1 cloned tables' owners: {sorted(set(owners.values()))} ({len(owners)} tables)")
        # PREDICTION: a schema clone keeps each child's OWNER (the transformer), not the cloner.
        check("B1 schema clone: child tables owned by",
              sorted(set(owners.values())), ["LEDGERLINE_TRANSFORMER"])
        cur.execute(f"SHOW FUTURE GRANTS IN SCHEMA {CI}.S16_GOLD")
        check("B1 schema clone: the source's future grants copied", len(cur.fetchall()), 0)
        options = shown(cur, f"SHOW SCHEMAS LIKE 'S16_GOLD' IN DATABASE {CI}", "options")
        check("B1 schema clone: still MANAGED ACCESS", options.get("S16_GOLD"), "MANAGED ACCESS")
        b2 = attempt(cur, f"CREATE OR REPLACE TABLE {CI}.S16_GOLD.FCT_ORDERS CLONE {PROD}")
        print(f"  B2 replace a cloned table (what a CI build does to a model): {b2}")
        check("B2 the CI role may replace a table in its cloned schema", b2 == "ok", False)

    cur.execute(f"CREATE SCHEMA {CI}.S16_TABLES")
    b3 = attempt(cur, f"CREATE TABLE {CI}.S16_TABLES.FCT_ORDERS CLONE {PROD}")
    owner = shown(cur, f"SHOW TABLES IN SCHEMA {CI}.S16_TABLES", "owner").get("FCT_ORDERS")
    print(f"  B3 clone one table: {b3}; owner {owner}")
    check("B3 table clone: owner is the role that cloned", owner, "LEDGERLINE_CI_RUNNER")
    check("B3 the CI role may replace its own clone",
          attempt(cur, f"CREATE OR REPLACE TABLE {CI}.S16_TABLES.FCT_ORDERS CLONE {PROD}"), "ok")

    # B4 write-audit-publish. PUBLISHED stands in for production; AUDIT is where a run writes.
    pub, aud = f"{CI}.S16_PUBLISHED", f"{CI}.S16_AUDIT"
    cur.execute(f"CREATE SCHEMA {pub}")
    cur.execute(f"CREATE TABLE {pub}.FCT_ORDERS CLONE {PROD}")
    published_before = one(cur, f"SELECT COUNT(*), HASH_AGG(*) FROM {pub}.FCT_ORDERS")
    audit = f"SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM {aud}.FCT_ORDERS"  # audit: one row per order

    cur.execute(f"CREATE SCHEMA {aud} CLONE {pub}")  # WRITE goes here, never to PUBLISHED
    cur.execute(f"INSERT INTO {aud}.FCT_ORDERS SELECT * FROM {aud}.FCT_ORDERS LIMIT 1")  # a bad run
    dupes = one(cur, audit)[0]
    print(f"  B4 bad run: audit finds {dupes} duplicate order(s) -> not published")
    check("B4 bad run fails the audit", dupes, 1)
    check("B4 bad run: PUBLISHED unchanged",
          one(cur, f"SELECT COUNT(*), HASH_AGG(*) FROM {pub}.FCT_ORDERS") == published_before, True)

    cur.execute(f"CREATE OR REPLACE SCHEMA {aud} CLONE {pub}")  # a good run, from scratch
    cur.execute(f"""UPDATE {aud}.FCT_ORDERS SET status = 's16_published'
        WHERE order_id = (SELECT MIN(order_id) FROM {aud}.FCT_ORDERS)""")
    check("B4 good run passes the audit", one(cur, audit)[0], 0)
    cur.execute(f"ALTER SCHEMA {pub} SWAP WITH {aud}")  # PUBLISH: one metadata step
    check("B4 after SWAP: PUBLISHED holds the good run",
          one(cur, f"SELECT COUNT(*) FROM {pub}.FCT_ORDERS WHERE status = 's16_published'")[0], 1)
    check("B4 after SWAP: the old state is kept under the audit name (rollback = swap again)",
          one(cur, f"SELECT COUNT(*), HASH_AGG(*) FROM {aud}.FCT_ORDERS") == published_before, True)

    for s in B_SCHEMAS:
        cur.execute(f"DROP SCHEMA IF EXISTS {CI}.{s}")


def main() -> int:
    load_local_env(ROOT / ".env")
    conn = connect("LEDGERLINE_TRANSFORMER")
    try:
        cur = conn.cursor()
        cur.execute("USE WAREHOUSE LEDGERLINE_WH")
        print(f"transformer: secondary roles {one(cur, 'SELECT CURRENT_SECONDARY_ROLES()')[0]}")
        part_a(cur)
    finally:
        conn.close()
    conn = connect("LEDGERLINE_CI_RUNNER", as_ci=True)
    try:
        cur = conn.cursor()
        cur.execute("USE WAREHOUSE LEDGERLINE_WH")
        print(f"\nCI runner: {one(cur, 'SELECT CURRENT_USER(), CURRENT_SECONDARY_ROLES()')}")
        check("CI user cannot write production",
              attempt(cur, f"CREATE TABLE LEDGERLINE.GOLD.S16_PROBE CLONE {PROD}") == "ok", False)
        part_b(cur)
    finally:
        conn.close()
    wrong = [r for r in results if not r[3]]
    print(f"\n{len(results)} checks, {len(results) - len(wrong)} as predicted, {len(wrong)} not:")
    for label, got, predicted, _ in wrong:
        print(f"  WRONG {label}: got {got!r}, predicted {predicted!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
