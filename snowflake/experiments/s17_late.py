"""Session 17 experiment, exp_06: a change file that arrives LATE, below the facts' high-water mark.

    python snowflake/experiments/s17_late.py

fct_orders reads only changes NEWER than the newest version it holds (`silver_version`, the
high-water mark). That is right while files arrive in version order. If one file is skipped by
Snowpipe and loaded later (a missed S3 event, a file refused and fixed), its versions are already
BELOW the mark and the fact never reads them. Silver's real order changes (stg_orders, 118,028 rows,
versions 2..12) are replayed as three deliveries (models/experiments/exp17_changes.sql):

    delivery 1  versions 2..8     delivery 2  versions 11..12     delivery 3  version 9 (LATE)

A. the production pattern (mark on version, no lookback): the late file is skipped — the BUG,
   asserted present; the incremental fact differs from a full rebuild (= fct_equals_rebuild)
B. a lookback of k versions (`> mark - k`): k = 3 still misses version 9, k = 4 catches it — a
   lookback works only up to the lateness someone guessed, and re-reads rows on every run
C. a mark on ARRIVAL time: the late file is read whatever its versions; merge_changes' `>=` guard
   keeps the 2,868 orders that already hold a newer change from being overwritten by it
D. dbt microbatch (exp17_daily): lookback = 1 day picks up an event one day late, not one four
   days late, until those days are re-run with --event-time-start / --event-time-end

Predictions were computed from stg_orders before the first run (progress.md S17); a wrong one does
not stop the run — it is the finding. Runs as LEDGERLINE_TRANSFORMER; writes only in LEDGERLINE.S17.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from run_dbt import main as dbt  # noqa: E402
from sf import connect  # noqa: E402

from generators._common import load_local_env  # noqa: E402

S = "LEDGERLINE.S17"
ORDERS = 99_441  # orders in Silver (stg_orders, newest change per order; no order deletes)
NEWEST_IS_9 = 13_431  # orders whose newest change is version 9: what a skipped late file breaks
MISSING = 9_365  # ... of them created in version 9 and never changed again: absent from the fact
STALE = 4_066  # ... of them with an older change: present, with an old status
GUARDED = 2_868  # orders with a version 9 change AND a newer one: the late change must lose

results: list[tuple[str, object, object, bool]] = []


def check(label, got, predicted):
    ok = got == predicted
    results.append((label, got, predicted, ok))
    print(f"  {'ok   ' if ok else 'WRONG'} {label}: got {got!r}, predicted {predicted!r}")


def one(cur, sql: str):
    cur.execute(sql)
    return cur.fetchone()


def run_dbt(*args: str) -> None:
    if dbt(list(args)) != 0:
        raise SystemExit(f"dbt failed: {' '.join(args)}")


def build(model: str, step: int, lookback: int = 0, full: bool = False) -> None:
    flags = ["--full-refresh"] if full else []
    run_dbt("run", "--select", f"exp17_changes {model}", *flags,
            "--vars", f"{{exp17: true, exp17_step: {step}, exp17_lookback: {lookback}}}")


def truth() -> str:
    """The fact a full rebuild over the delivered log gives: each order's newest change."""
    return f"""(SELECT order_id, status, commit_version AS silver_version FROM {S}.EXP17_CHANGES
               QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY commit_version DESC) = 1)"""


def differs(cur, table: str) -> int:
    """Orders whose (status, version) differ from the full rebuild, either side missing included."""
    return one(cur, f"""
        SELECT COUNT(*) FROM {table} f FULL OUTER JOIN {truth()} t ON f.order_id = t.order_id
        WHERE f.order_id IS NULL OR t.order_id IS NULL
           OR f.status IS DISTINCT FROM t.status OR f.silver_version <> t.silver_version""")[0]


def version_run(cur, lookback: int) -> dict:
    """Steps 1-3 for the version-mark fact; rows read by each incremental run, and the damage."""
    table = f"{S}.EXP17_FCT_VERSION"
    build("exp17_fct_version", 1, lookback, full=True)
    read = {}
    for step in (2, 3):
        mark = one(cur, f"SELECT MAX(silver_version) FROM {table}")[0]
        build("exp17_fct_version", step, lookback)
        read[step] = one(cur, f"SELECT COUNT(*) FROM {S}.EXP17_CHANGES "
                              f"WHERE commit_version > {mark} - {lookback}")[0]
    rows = one(cur, f"SELECT COUNT(*) FROM {table}")[0]
    v9 = one(cur, f"SELECT COUNT(*) FROM {table} WHERE silver_version = 9")[0]
    return {"read": read, "rows": rows, "v9": v9, "differs": differs(cur, table)}


def part_abc(cur) -> None:
    print("\n=== A. mark on version, no lookback (production today): delivery 3 (version 9) is late")
    a = version_run(cur, 0)
    check("A rows read by the late run", a["read"][3], 0)
    check("A fact rows (version-9-only orders absent)", a["rows"], ORDERS - MISSING)
    check("A orders differing from a full rebuild (the bug, present)", a["differs"], NEWEST_IS_9)
    check("A rows holding version 9", a["v9"], 0)

    print("\n=== B. lookback of k versions: k = 3 (reads > 9), k = 4 (reads > 8)")
    b3 = version_run(cur, 3)
    check("B k=3 orders differing (still misses version 9)", b3["differs"], NEWEST_IS_9)
    b4 = version_run(cur, 4)
    check("B k=4 orders differing (catches it)", b4["differs"], 0)
    check("B k=4 rows holding version 9", b4["v9"], NEWEST_IS_9)
    print(f"  cost: rows read by the step 2 / step 3 runs — k=0 {a['read']}, k=3 {b3['read']}, "
          f"k=4 {b4['read']}")
    # Every run with k = 4 re-reads the last four versions whether anything was late or not.
    check("B k=4 step 3 re-reads more than the late file alone (16,299 rows)", b4["read"][3] > 16_299, True)

    print("\n=== C. mark on arrival time")
    table = f"{S}.EXP17_FCT_ARRIVAL"
    build("exp17_fct_arrival", 1, full=True)
    build("exp17_fct_arrival", 2)
    newer_before = one(cur, f"SELECT COUNT(*) FROM {table} WHERE silver_version > 9")[0]
    build("exp17_fct_arrival", 3)
    check("C orders differing from a full rebuild", differs(cur, table), 0)
    check("C rows holding version 9", one(cur, f"SELECT COUNT(*) FROM {table} WHERE silver_version = 9")[0],
          NEWEST_IS_9)
    newer_after = one(cur, f"SELECT COUNT(*) FROM {table} WHERE silver_version > 9")[0]
    check("C orders on version 11/12 untouched by the late version 9", newer_after, newer_before)
    guarded = one(cur, f"""
        SELECT COUNT(*) FROM {table} f
        WHERE f.silver_version > 9
          AND EXISTS (SELECT 1 FROM {S}.EXP17_CHANGES c
                      WHERE c.order_id = f.order_id AND c.commit_version = 9)""")[0]
    check("C orders that kept a newer change over the late one (the >= guard)", guarded, GUARDED)


def part_d(cur) -> None:
    print("\n=== D. dbt microbatch, batch = 1 day, lookback = 1")
    today = datetime.now(UTC).date()
    day = {k: today - timedelta(days=k) for k in range(5)}
    table = f"{S}.EXP17_DAILY"

    def counts() -> dict:
        cur.execute(f"SELECT event_day::date, events FROM {table} ORDER BY 1")
        return dict(cur.fetchall())

    run_dbt("run", "--select", "exp17_events exp17_daily", "--full-refresh",
            "--vars", "{exp17: true, exp17_step: 1}")
    check("D step 1: two events a day, 4 days", counts(), {day[k]: 2 for k in (4, 3, 2, 1)})

    # Step 2: the log gains one event for yesterday and one for today-4. A normal run on the
    # schedule: today's batch plus one day of lookback = yesterday and today.
    run_dbt("run", "--select", "exp17_events exp17_daily", "--vars", "{exp17: true, exp17_step: 2}")
    c = counts()
    check("D step 2: yesterday's late event counted (inside the lookback)", c.get(day[1]), 3)
    check("D step 2: today-4's late event NOT counted (outside it)", c.get(day[4]), 2)

    # Step 3: re-run exactly the days the late event belongs to.
    start, end = day[4].isoformat(), day[3].isoformat()
    run_dbt("run", "--select", "exp17_daily", "--event-time-start", start, "--event-time-end", end,
            "--vars", "{exp17: true, exp17_step: 2}")
    check("D step 3: today-4 re-run, its late event counted", counts().get(day[4]), 3)
    check("D step 3: other days untouched", {d: n for d, n in counts().items() if d != day[4]},
          {day[3]: 2, day[2]: 2, day[1]: 3})


def main() -> int:
    load_local_env(ROOT / ".env")
    conn = connect(role="LEDGERLINE_TRANSFORMER")
    try:
        cur = conn.cursor()
        cur.execute("USE WAREHOUSE LEDGERLINE_WH")
        part_abc(cur)
        part_d(cur)
    finally:
        conn.close()
    wrong = [r for r in results if not r[3]]
    print(f"\n{len(results) - len(wrong)} of {len(results)} predictions held")
    for label, got, predicted, _ in wrong:
        print(f"  WRONG {label}: got {got!r}, predicted {predicted!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
