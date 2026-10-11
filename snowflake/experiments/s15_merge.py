"""Session 15 experiment: deletes on Snowflake — a snapshot by anti-join, a change log by its flag.

    python snowflake/experiments/s15_merge.py

Snowflake's MERGE has no `WHEN NOT MATCHED BY SOURCE` (Delta has; Silver dims use it, Session 8).
Two questions, both about where a delete comes from:

A. A SNAPSHOT source (seller, night by night: everything that exists tonight). A row missing from
   the snapshot is a delete. Applied to a scratch table in S15 two ways, every night, and compared
   with what Silver held after Delta's `NOT MATCHED BY SOURCE DELETE` (the change log as of night n):
     A1  MERGE (insert / update) + DELETE ... WHERE NOT EXISTS (the anti-join), one transaction
     A2  ONE MERGE whose source is the snapshot FULL OUTER JOIN the target: a target row with no
         snapshot row comes through flagged `gone`, and `WHEN MATCHED AND gone THEN DELETE`
     A3  the trap — CLAUDE.md rule 1 on Snowflake: the anti-join fed one night's CHANGE rows
         instead of the snapshot; then the right way for a change batch (read the flag)

B. A CHANGE-LOG source (the facts): deletes arrive as rows. Two dbt incremental models over the
   same tiny log (models/experiments/exp15_*): the project's `merge_changes` strategy and dbt's own
   `merge`, steps 1 (first build), 2 (an update, a delete, a delete for a never-loaded order),
   3 (step 2 again: idempotent?).

Runs as LEDGERLINE_TRANSFORMER (key pair). Writes only in LEDGERLINE.S15; reads STAGING. Safe to
re-run: drops its own tables first. Predictions were written before the first run; a wrong one
does not stop the run — it is the finding (Session 11's rule).
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from run_dbt import main as dbt  # noqa: E402
from sf import connect  # noqa: E402

from generators._common import load_local_env  # noqa: E402

S = "LEDGERLINE.S15"
STG = "LEDGERLINE.STAGING.STG_SELLER"
KEY = "seller_id"
ATTRS = ["seller_zip_code_prefix", "seller_city", "seller_state", "dim_updated_at"]
COLS = [KEY, *ATTRS]

results: list[tuple[str, object, object, bool]] = []


def check(label, got, predicted):
    ok = got == predicted
    results.append((label, got, predicted, ok))
    print(f"  {'ok   ' if ok else 'WRONG'} {label}: got {got!r}, predicted {predicted!r}")


def snapshot(n: int) -> str:
    """Silver's seller table as it stood after night n — what Delta's NOT MATCHED BY SOURCE left."""
    return f"""(
        SELECT {", ".join(COLS)} FROM (
            SELECT * FROM {STG} WHERE night <= {n}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY {KEY} ORDER BY commit_version DESC) = 1)
        WHERE change_type <> 'delete')"""


def changed(alias_t: str = "t", alias_s: str = "s") -> str:
    return " OR ".join(f"{alias_t}.{c} IS DISTINCT FROM {alias_s}.{c}" for c in ATTRS)


def set_all(alias_s: str = "s") -> str:
    return ", ".join(f"{c} = {alias_s}.{c}" for c in ATTRS)


def insert_all(alias_s: str = "s") -> str:
    return f"({', '.join(COLS)}) VALUES ({', '.join(f'{alias_s}.{c}' for c in COLS)})"


def one(cur, sql: str):
    cur.execute(sql)
    return cur.fetchone()


def diff(cur, table: str, n: int) -> tuple[int, int]:
    """(rows only in the table, rows only in Silver's night-n state) — (0, 0) = identical."""
    cols, night = ", ".join(COLS), snapshot(n)
    extra = one(cur, f"SELECT COUNT(*) FROM (SELECT {cols} FROM {table} MINUS SELECT * FROM {night})")[0]
    missing = one(cur, f"SELECT COUNT(*) FROM (SELECT * FROM {night} MINUS SELECT {cols} FROM {table})")[0]
    return extra, missing


def create(cur, table: str) -> None:
    cur.execute(f"""CREATE OR REPLACE TABLE {table} (
        seller_id STRING, seller_zip_code_prefix STRING, seller_city STRING, seller_state STRING,
        dim_updated_at TIMESTAMP_NTZ)""")


# ---------------------------------------------------------------------------------------------
# A. Snapshot source: delete by absence, without NOT MATCHED BY SOURCE
# ---------------------------------------------------------------------------------------------


def a1_night(cur, table: str, n: int) -> tuple[int, int, int]:
    """MERGE then anti-join DELETE, one transaction. Returns (inserted, updated, deleted)."""
    cur.execute("BEGIN")
    ins, upd = one(cur, f"""
        MERGE INTO {table} t USING {snapshot(n)} s ON t.{KEY} = s.{KEY}
        WHEN MATCHED AND ({changed()}) THEN UPDATE SET {set_all()}
        WHEN NOT MATCHED THEN INSERT {insert_all()}""")
    deleted = one(cur, f"""
        DELETE FROM {table} t
        WHERE NOT EXISTS (SELECT 1 FROM {snapshot(n)} s WHERE s.{KEY} = t.{KEY})""")[0]
    cur.execute("COMMIT")
    return ins, upd, deleted


def a2_night(cur, table: str, n: int) -> tuple[int, int, int]:
    """One MERGE: the source carries the target's absent rows, flagged `gone`."""
    s_cols = ", ".join(f"s.{c}" for c in ATTRS)
    return one(cur, f"""
        MERGE INTO {table} t
        USING (
            SELECT COALESCE(s.{KEY}, cur.{KEY}) AS {KEY}, {s_cols}, s.{KEY} IS NULL AS gone
            FROM {snapshot(n)} s FULL OUTER JOIN {table} cur ON cur.{KEY} = s.{KEY}
        ) s ON t.{KEY} = s.{KEY}
        WHEN MATCHED AND s.gone THEN DELETE
        WHEN MATCHED AND ({changed()}) THEN UPDATE SET {set_all()}
        WHEN NOT MATCHED AND NOT s.gone THEN INSERT {insert_all()}""")


def part_a(cur) -> None:
    nights = one(cur, f"SELECT MAX(night) FROM {STG}")[0]
    print(f"\n=== A. seller snapshot, nights 1..{nights}: MERGE + anti-join vs Delta's NOT MATCHED BY SOURCE")
    # From the generator's report (progress.md S8 / S15): 100 sellers deleted on every night from
    # night 5 (2017-12-28) on; none before. Inserts only on night 1 (3,095).
    deletes = {n: (100 if n >= 5 else 0) for n in range(1, nights + 1)}
    for name, step in (("A1", a1_night), ("A2", a2_night)):
        table = f"{S}.SELLER_{name}"
        create(cur, table)
        for n in range(1, nights + 1):
            ins, upd, dele = step(cur, table, n)
            print(f"  {name} night {n}: +{ins:,} ~{upd:,} -{dele:,}")
            check(f"{name} night {n} deleted", dele, deletes[n])
            check(f"{name} night {n} = Silver after Delta's MERGE (extra, missing)",
                  diff(cur, table, n), (0, 0))

    print("\n=== A3. the trap: the anti-join fed ONE night's change rows (night 5) on night 4's state")
    trap, right = f"{S}.SELLER_TRAP", f"{S}.SELLER_FLAG"
    for table in (trap, right):
        create(cur, table)
        cur.execute(f"INSERT INTO {table} SELECT * FROM {snapshot(4)}")
    batch = f"(SELECT * FROM {STG} WHERE night = 5)"  # what a CDC batch is: only what changed
    kinds = dict(cur.execute(f"SELECT change_type, COUNT(*) FROM {batch} GROUP BY 1").fetchall())
    print(f"  night 5's change rows: {kinds}")
    # PREDICTION: the MERGE does not read change_type, so the 100 delete rows (which carry the
    # deleted sellers' last values) match and change nothing; the anti-join keeps only keys IN the
    # batch: 3,095 - 150 = 2,945 sellers deleted, 150 left — 100 of them the ones Silver deleted.
    cur.execute("BEGIN")
    upd = one(cur, f"""MERGE INTO {trap} t USING {batch} s ON t.{KEY} = s.{KEY}
        WHEN MATCHED AND ({changed()}) THEN UPDATE SET {set_all()}
        WHEN NOT MATCHED THEN INSERT {insert_all()}""")[1]
    dele = one(cur, f"""DELETE FROM {trap} t
        WHERE NOT EXISTS (SELECT 1 FROM {batch} s WHERE s.{KEY} = t.{KEY})""")[0]
    cur.execute("COMMIT")
    rows = one(cur, f"SELECT COUNT(*) FROM {trap}")[0]
    kept_deleted = one(cur, f"""SELECT COUNT(*) FROM {trap} t
        JOIN {batch} b ON b.{KEY} = t.{KEY} AND b.change_type = 'delete'""")[0]
    check("A3 trap: updated", upd, 50)
    check("A3 trap: deleted by the anti-join", dele, 2_945)
    check("A3 trap: rows left", rows, 150)
    check("A3 trap: rows left that Silver deleted", kept_deleted, 100)
    check("A3 trap vs Silver night 5 (extra, missing)", diff(cur, trap, 5), (100, 2_945))

    # The right way for a change batch: read the flag (Silver inventory's MERGE, CLAUDE.md rule 1).
    ins, upd, dele = one(cur, f"""
        MERGE INTO {right} t USING {batch} s ON t.{KEY} = s.{KEY}
        WHEN MATCHED AND s.change_type = 'delete' THEN DELETE
        WHEN MATCHED AND s.change_type <> 'delete' AND ({changed()}) THEN UPDATE SET {set_all()}
        WHEN NOT MATCHED AND s.change_type <> 'delete' THEN INSERT {insert_all()}""")
    check("A3 by the flag: (inserted, updated, deleted)", (ins, upd, dele), (0, 50, 100))
    check("A3 by the flag = Silver night 5 (extra, missing)", diff(cur, right, 5), (0, 0))


# ---------------------------------------------------------------------------------------------
# B. Change-log source: dbt's merge vs the project's merge_changes
# ---------------------------------------------------------------------------------------------


def run_dbt(*args: str) -> None:
    if dbt(list(args)) != 0:
        raise SystemExit(f"dbt failed: {' '.join(args)}")


def state(cur, model: str) -> list[tuple]:
    cur.execute(f"SELECT order_id, status, silver_version, silver_change_type FROM {S}.{model} ORDER BY 1")
    return [tuple(r) for r in cur.fetchall()]


def part_b(cur) -> None:
    print("\n=== B. a change log with in-band deletes: dbt 'merge' vs 'merge_changes'")
    models = ["exp15_changes", "exp15_fct_custom", "exp15_fct_builtin"]
    first = [("O1", "created", 3, "insert"), ("O2", "created", 2, "insert"), ("O3", "created", 4, "insert")]

    run_dbt("run", "--select", *models, "--full-refresh", "--vars", "{exp15: true, exp15_step: 1}")
    for m in models[1:]:
        check(f"B step 1 {m}", state(cur, m), first)

    run_dbt("run", "--select", *models, "--vars", "{exp15: true, exp15_step: 2}")
    # PREDICTION: merge_changes deletes O2 and ignores O4's delete; dbt's merge UPDATEs O2 into a
    # row saying 'delete' and INSERTs O4 — 4 rows, two of them deletions kept as data.
    check("B step 2 merge_changes", state(cur, "EXP15_FCT_CUSTOM"),
          [("O1", "shipped", 7, "update_postimage"), ("O3", "created", 4, "insert")])
    check("B step 2 dbt merge", state(cur, "EXP15_FCT_BUILTIN"),
          [("O1", "shipped", 7, "update_postimage"), ("O2", "created", 9, "delete"),
           ("O3", "created", 4, "insert"), ("O4", "created", 8, "delete")])

    before = {m: state(cur, m) for m in models[1:]}
    reread = one(cur, f"""SELECT COUNT(*) FROM {S}.EXP15_CHANGES
        WHERE commit_version > (SELECT MAX(silver_version) FROM {S}.EXP15_FCT_CUSTOM)""")[0]
    run_dbt("run", "--select", *models, "--vars", "{exp15: true, exp15_step: 2}")
    for m in models[1:]:
        check(f"B step 3 (step 2 again) {m} unchanged", state(cur, m) == before[m], True)
    # The high-water mark is read from the fact; a deleted row leaves it, so the newest versions —
    # the deletes, v8 and v9 — sit above the mark and are read again on every run (and ignored).
    check("B step 3 change rows above merge_changes' high-water mark (read again, changing nothing)",
          reread, 2)


def main() -> int:
    load_local_env(ROOT / ".env")
    conn = connect("LEDGERLINE_TRANSFORMER")
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE LEDGERLINE_WH")
    print(f"secondary roles: {one(cur, 'SELECT CURRENT_SECONDARY_ROLES()')[0]}")
    try:
        part_a(cur)
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
