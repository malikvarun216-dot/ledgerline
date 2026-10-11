"""exp_05 (Session 14): SCD2 point in time — run time vs business time, and what a delete does.

    python snowflake/experiments/exp_05_scd2.py

The question a type-2 dimension answers is "what did we know about this customer AT time T?".
This experiment asks it at each of the five dump nights, of five dbt snapshots of the customer
dimension built in the scratch schema LEDGERLINE.S14, and checks every answer against an answer key
that owes nothing to Databricks or dbt: the dims generator, re-run in memory with the arguments the
real dumps were landed with (progress.md, Session 8), which rebuilds each night's customer dump
from the Olist CSVs.

  runtime_check   strategy='check': dbt_valid_from = when the snapshot RAN       (the bug)
  ts_ignore       strategy='timestamp' on dim_updated_at, hard_deletes='ignore'  (dbt's default)
  ts_invalidate   ... hard_deletes='invalidate'
  ts_new_record   ... hard_deletes='new_record'
  ts_one_run      timestamp + invalidate, but run ONCE on today's state, no replay (a late start)

The first four are built by replaying the nights: `dbt snapshot --vars "{as_of_night: n}"` for
n = 1..5, so each run sees Silver as it stood after the n-th dump. Every run happens today, minutes
apart — which is exactly what makes run time and business time come apart.

Runs from the laptop as LEDGERLINE_TRANSFORMER (dbt and scripts/sf.py, key pair). Writes only in S14;
reads RAW through staging. Safe to re-run: it drops its own S14 tables first. Each check prints
what Snowflake answered next to what was PREDICTED before the first run; a wrong prediction does
not stop the run — it is the finding (Session 11's rule).
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from run_dbt import main as dbt  # noqa: E402
from sf import connect  # noqa: E402

from generators import dim_dumps, olist  # noqa: E402
from generators._common import load_local_env  # noqa: E402

S = "LEDGERLINE.S14"
KEY = "customer_unique_id"
ATTRS = ["customer_zip_code_prefix", "customer_city", "customer_state", "dim_updated_at"]
REPLAYED = ["runtime_check", "ts_ignore", "ts_invalidate", "ts_new_record"]
ALL = [*REPLAYED, "ts_one_run"]

# The landed dims (progress.md S8): first order date, 5 nights, 120 days apart, 100 deletions per
# night from 2017-12-28 on. Generator defaults otherwise (seed 20260919, change rate 25).
START, NIGHTS, STRIDE = date(2016, 9, 4), 5, 120
DELETE_PER_NIGHT, DELETE_FROM = 100, date(2017, 12, 28)
NIGHT_DATES = [START + timedelta(days=STRIDE * k) for k in range(NIGHTS)]

results: list[tuple[str, object, object, bool]] = []


def check(label, got, predicted):
    ok = got == predicted
    results.append((label, got, predicted, ok))
    print(f"  {'ok   ' if ok else 'WRONG'} {label}: got {got!r}, predicted {predicted!r}")


# ---------------------------------------------------------------------------------------------
# The answer key: each night's customer dump, rebuilt by the generator in memory
# ---------------------------------------------------------------------------------------------


class MemoryBlobSink:
    def __init__(self) -> None:
        self.blobs: dict[str, str] = {}

    def put_text(self, key: str, text: str) -> None:
        self.blobs[key] = text

    def get_text(self, key: str) -> str:
        return self.blobs[key]

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(k for k in self.blobs if k.startswith(prefix))


def answer_key() -> dict[int, pd.DataFrame]:
    """{night 1..5: the customer dump of that night}, keyed by customer_unique_id."""
    frames = {n: olist.load(n, olist.DEFAULT_RAW_DIR) for n in ("customers", "orders", "products", "sellers")}
    sink = MemoryBlobSink()
    dim_dumps.run(
        frames,
        sink,
        start=START,
        nights=NIGHTS,
        stride_days=STRIDE,
        delete_per_night=DELETE_PER_NIGHT,
        delete_from=DELETE_FROM,
    )
    nights = {}
    for n, night_date in enumerate(NIGHT_DATES, start=1):
        text = sink.get_text(dim_dumps.dump_key("customer", night_date))
        nights[n] = normalise(pd.read_csv(StringIO(text), dtype=str))
    return nights


def normalise(df: pd.DataFrame) -> pd.DataFrame:
    """Same shape on both sides: zip as a number (the dumps lost the leading zero; Silver pads it
    back — decisions.md S8), the timestamp as a pandas Timestamp, the key as the index."""
    out = df[[KEY, *ATTRS]].copy()
    out["customer_zip_code_prefix"] = pd.to_numeric(out["customer_zip_code_prefix"]).astype("int64")
    out["dim_updated_at"] = pd.to_datetime(out["dim_updated_at"])
    return out.set_index(KEY).sort_index()


def compare(got: pd.DataFrame, want: pd.DataFrame) -> tuple[int, int, int]:
    """(missing, extra, different) — keys the snapshot lacks, keys it has that the dump does not,
    and keys on both sides whose values differ."""
    missing = len(want.index.difference(got.index))
    extra = len(got.index.difference(want.index))
    both = want.index.intersection(got.index)
    different = int((got.loc[both, ATTRS] != want.loc[both, ATTRS]).any(axis=1).sum())
    return missing, extra, different


# ---------------------------------------------------------------------------------------------
# Building the snapshots
# ---------------------------------------------------------------------------------------------


def run_dbt(*args: str) -> None:
    if dbt(list(args)) != 0:
        raise SystemExit(f"dbt failed: {' '.join(args)}")


def now(cur) -> datetime:
    cur.execute("SELECT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ")
    return cur.fetchone()[0]


def build(cur) -> dict[int, tuple[datetime, datetime]]:
    """Drop the S14 tables, replay nights 1..5 into the four replayed snapshots, then run ts_one_run
    once on today's state. Returns {night: (started, finished)} in Snowflake's clock."""
    for v in ALL:
        cur.execute(f"DROP TABLE IF EXISTS {S}.EXP05_{v.upper()}")
    windows = {}
    for n in range(1, NIGHTS + 1):
        print(f"\n--- night {n} ({NIGHT_DATES[n - 1]}): dbt snapshot, as_of_night = {n}")
        started = now(cur)
        run_dbt(
            "snapshot",
            "--select",
            *[f"exp05_{v}" for v in REPLAYED],
            "--vars",
            f"{{exp05: true, as_of_night: {n}}}",
        )
        windows[n] = (started, now(cur))
    print("\n--- ts_one_run: one dbt snapshot of today's state")
    run_dbt("snapshot", "--select", "exp05_ts_one_run", "--vars", "{exp05: true}")
    return windows


def as_of(cur, variant: str, ts: str) -> pd.DataFrame:
    """The dimension as this snapshot says it stood at `ts` — the point-in-time question."""
    not_deleted = "AND COALESCE(dbt_is_deleted, 'False') = 'False'" if variant == "ts_new_record" else ""
    cur.execute(f"""
        SELECT {KEY}, {", ".join(ATTRS)}
        FROM {S}.EXP05_{variant.upper()}
        WHERE dbt_valid_from <= {ts} AND {ts} < dbt_valid_to {not_deleted}""")
    df = pd.DataFrame(cur.fetchall(), columns=[KEY, *ATTRS])
    return normalise(df)


def end_of(night_date: date) -> str:
    # The generator's dump for date D holds what was true at D 23:59:59.999999.
    return f"'{night_date} 23:59:59.999999'::TIMESTAMP_NTZ"


# ---------------------------------------------------------------------------------------------


def main() -> int:
    load_local_env(ROOT / ".env")
    print("Answer key: the generator rebuilds the five customer dumps in memory ...")
    key = answer_key()
    for n, df in key.items():
        print(f"  night {n} {NIGHT_DATES[n - 1]}: {len(df):,} customers")
    deleted = key[4].index.difference(key[5].index)  # in night 4, gone from night 5
    print(f"  deleted on night 5: {len(deleted)}")

    conn = connect("LEDGERLINE_TRANSFORMER")
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE LEDGERLINE_WH")
    try:
        windows = build(cur)

        # ---- PREDICTIONS (written before the first run) -------------------------------------
        # runtime_check: every version is stamped with today's run time, so asked about any 2017
        #   night it knows nobody: everyone missing.
        # timestamp variants, replayed: nights 1-4 exact. Night 5: the 19 deleted people are still
        #   there — for 'ignore' because nothing closes them, and ALSO for 'invalidate' and
        #   'new_record', because the close time is the run time (2026), after 2017-12-28.
        # ts_one_run: night 5 exact (it IS night 5's state); for nights 1-4 everyone whose row
        #   changed later, or who was deleted, is missing — their only version starts at their
        #   newest dim_updated_at, after that night. Nobody extra, nobody different.
        predicted = {}
        for n in range(1, NIGHTS + 1):
            dump = key[n]
            predicted[("runtime_check", n)] = (len(dump), 0, 0)
            for v in ("ts_ignore", "ts_invalidate", "ts_new_record"):
                predicted[(v, n)] = (0, len(deleted) if n == 5 else 0, 0)
            later = dump.index.difference(key[5].index).union(
                dump.index.intersection(key[5].index)[
                    (
                        dump.loc[dump.index.intersection(key[5].index), ATTRS]
                        != key[5].loc[dump.index.intersection(key[5].index), ATTRS]
                    )
                    .any(axis=1)
                    .to_numpy()
                ]
            )
            predicted[("ts_one_run", n)] = (len(later), 0, 0)

        print("\nA. Point in time: each night asked of the snapshot vs that night's dump")
        print("   (missing, extra, different)")
        for v in ALL:
            for n in range(1, NIGHTS + 1):
                got = compare(as_of(cur, v, end_of(NIGHT_DATES[n - 1])), key[n])
                check(f"{v:<14} night {n}", got, predicted[(v, n)])

        print("\nB. Today: the snapshot's current rows vs Silver's current customers (night 5)")
        today = "CURRENT_TIMESTAMP()::TIMESTAMP_NTZ"
        check("ts_ignore     today", compare(as_of(cur, "ts_ignore", today), key[5]), (0, len(deleted), 0))
        check("ts_invalidate today", compare(as_of(cur, "ts_invalidate", today), key[5]), (0, 0, 0))
        check("ts_new_record today", compare(as_of(cur, "ts_new_record", today), key[5]), (0, 0, 0))

        print("\nC. When does each snapshot say the 19 deletions happened?")
        run5 = windows[NIGHTS]
        cur.execute(f"""SELECT COUNT(*), MIN(dbt_valid_to), MAX(dbt_valid_to) FROM {S}.EXP05_TS_INVALIDATE
                        WHERE dbt_valid_to < '9999-12-31' AND {KEY} IN (
                          SELECT {KEY} FROM {S}.EXP05_TS_INVALIDATE
                          GROUP BY 1 HAVING MAX(dbt_valid_to) < '9999-12-31')""")
        n_closed, lo, hi = cur.fetchone()
        print(f"  invalidate: {n_closed} rows closed for good, dbt_valid_to {lo} .. {hi}")
        print(f"  the night-5 run: {run5[0]} .. {run5[1]}")
        check("invalidate closes the deleted", n_closed, len(deleted))
        in_run5 = bool(lo and run5[0] <= lo and hi <= run5[1])
        check("... at the night-5 RUN time, not 2017-12-28", in_run5, True)
        cur.execute(
            f"SELECT COUNT(*), MIN(dbt_valid_from) FROM {S}.EXP05_TS_NEW_RECORD WHERE dbt_is_deleted = 'True'"
        )
        n_del, del_from = cur.fetchone()
        print(f"  new_record: {n_del} deletion rows, valid from {del_from}")
        check("new_record adds one deletion row each", n_del, len(deleted))
        check("... valid from the night-5 RUN time", bool(del_from and run5[0] <= del_from <= run5[1]), True)

        print("\nD. How much history each one kept (rows, people, rows per changed person)")
        for v in ALL:
            cur.execute(f"SELECT COUNT(*), COUNT(DISTINCT {KEY}) FROM {S}.EXP05_{v.upper()}")
            rows, people = cur.fetchone()
            print(f"  {v:<14} {rows:>7,} rows  {people:>7,} people  {rows - people:>4} extra versions")
        cur.execute(f"SELECT COUNT(*) - COUNT(DISTINCT {KEY}) FROM {S}.EXP05_TS_ONE_RUN")
        check("ts_one_run keeps no history (one row per person)", cur.fetchone()[0], 0)

        print("\nE. SCD2 shape on ts_invalidate: one open row per person, no overlapping versions")
        cur.execute(f"""SELECT COUNT(*) FROM (
                          SELECT {KEY} FROM {S}.EXP05_TS_INVALIDATE WHERE dbt_valid_to = '9999-12-31'
                          GROUP BY 1 HAVING COUNT(*) > 1)""")
        check("people with two open rows", cur.fetchone()[0], 0)
        cur.execute(f"""SELECT COUNT(*) FROM (
                          SELECT {KEY}, dbt_valid_to,
                                 LEAD(dbt_valid_from) OVER (PARTITION BY {KEY} ORDER BY dbt_valid_from)
                                   AS next_from
                          FROM {S}.EXP05_TS_INVALIDATE)
                        WHERE next_from IS NOT NULL AND next_from <> dbt_valid_to""")
        check("versions that do not meet end to start", cur.fetchone()[0], 0)
    finally:
        conn.close()

    wrong = [r for r in results if not r[3]]
    print(f"\n{len(results)} checks, {len(results) - len(wrong)} as predicted, {len(wrong)} not.")
    for label, got, pred, _ in wrong:
        print(f"  WRONG {label}: got {got!r}, predicted {pred!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
