"""One step of the nightly pipeline — what each Airflow task runs, and what a person runs by hand.

    python scripts/pipeline_step.py nights                          # the landed nights' dump dates
    python scripts/pipeline_step.py land --night 2017-05-02         # land nights 1..n (write-once)
    python scripts/pipeline_step.py job ledgerline-silver           # run a Databricks job, wait
    python scripts/pipeline_step.py job ledgerline-bridge           #   (prints the notebook's exit value)
    python scripts/pipeline_step.py raw-has '<export summary json>' # exit 0 once RAW holds it all
    python scripts/pipeline_step.py check-raw                       # 12_check_raw.sql: empty = pass

decisions.md, Session 16 "Airflow runs one DAG per dims night". Every step is safe to repeat — that
is what lets Airflow retry a task, or backfill a night, without a second thought:

* ``land``: the dims generator lands nights 1..n write-once; an already landed night is compared
  byte for byte and left alone ("identical"); a changed one is REFUSED (decisions.md, S8).
* ``job``: the Databricks jobs are idempotent by their own logs (merge log, export log).
* ``raw-has`` / ``check-raw``: read only.

Credentials, all from ``.env`` (or the container's environment): Databricks as the ``ledgerline-ci``
service principal (DATABRICKS_HOST / CLIENT_ID / CLIENT_SECRET); Snowflake as LEDGERLINE_SVC; S3
with the project's scoped key.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from generators._common import load_local_env  # noqa: E402

# The landing schedule, exactly as every landing since Session 8 ran it (progress.md, S8 / S15).
FIRST_NIGHT = date(2016, 9, 4)  # the first Olist order's date: dim_dumps' default start
STRIDE_DAYS = 120
LANDED = 8  # nights 1..8 are in the landing zone (Session 15)
LANDING_FLAGS = [
    "--prefix", "ledgerline", "--stride-days", str(STRIDE_DAYS),
    "--delete-per-night", "100", "--delete-from", "2017-12-28", "--pii-from", "2017-08-30",
]
RAW = "LEDGERLINE.RAW.SILVER_CHANGES"


def night_dates(n: int = LANDED) -> list[date]:
    return [FIRST_NIGHT + timedelta(days=STRIDE_DAYS * i) for i in range(n)]


def night_number(night: date) -> int:
    for i, d in enumerate(night_dates(LANDED + 50), start=1):
        if d == night:
            return i
    raise SystemExit(f"{night} is not a dims night (every {STRIDE_DAYS} days from {FIRST_NIGHT})")


def land(night: date) -> int:
    n = night_number(night)
    bucket = os.environ.get("LEDGERLINE_S3_BUCKET")
    if not bucket:
        raise SystemExit("LEDGERLINE_S3_BUCKET missing")
    cmd = [sys.executable, "generators/dim_dumps.py", "--sink", "s3", "--bucket", bucket,
           "--nights", str(n), *LANDING_FLAGS]
    print(f"night {n} = {night}: {' '.join(cmd[1:])}", flush=True)
    return subprocess.run(cmd, cwd=ROOT).returncode


def run_job(name: str) -> int:
    """Start a Databricks job by name as the ledgerline-ci service principal and wait for it. The
    last line printed is the job's last notebook exit value (or {}), for the next Airflow task."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import RunResultState

    w = WorkspaceClient()  # DATABRICKS_HOST + CLIENT_ID + CLIENT_SECRET: OAuth M2M
    print(f"as {w.current_user.me().user_name}", flush=True)
    jobs = list(w.jobs.list(name=name))
    if len(jobs) != 1:
        raise SystemExit(f"expected one job named {name!r}, found {len(jobs)} — is it deployed, and "
                         "may this service principal see it (CAN_MANAGE_RUN)?")
    run = w.jobs.run_now_and_wait(jobs[0].job_id, timeout=timedelta(minutes=60))
    state = run.state.result_state
    print(f"{name}: run {run.run_id} {state.value if state else run.state.life_cycle_state} "
          f"{run.run_page_url}", flush=True)
    if state != RunResultState.SUCCESS:
        return 1
    output = {}
    for task in sorted(run.tasks or [], key=lambda t: t.end_time or 0):
        result = w.jobs.get_run_output(task.run_id).notebook_output
        if result and result.result:
            output = json.loads(result.result)
    print(json.dumps(output, sort_keys=True))
    return 0


def snowflake_rows(sql: str) -> list[tuple]:
    from sf import connect

    conn = connect("LEDGERLINE_LOADER")
    try:
        cur = conn.cursor()
        cur.execute("USE WAREHOUSE LEDGERLINE_WH")
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def raw_has(summary: str) -> int:
    """Exit 0 when RAW holds exactly the rows the bridge logged for every exported folder.

    ``summary`` is the bridge job's exit value: {"<table>/v<from>-<to>": rows, ...} for EVERY folder
    in its export log, not only this run's — so a retried run (whose export finds nothing new) still
    waits for everything before it, instead of passing on an empty list."""
    expected = json.loads(summary or "{}")
    if not expected:
        print("the bridge reported no folders — nothing to wait for")
        return 0
    loaded = dict(snowflake_rows(f"""
        SELECT REGEXP_SUBSTR(SOURCE_FILE, '([^/]+/v[0-9]{{10}}-[0-9]{{10}})/', 1, 1, 'e', 1), COUNT(*)
        FROM {RAW} GROUP BY 1"""))
    short = {f: (rows, loaded.get(f, 0)) for f, rows in expected.items() if loaded.get(f, 0) != rows}
    print(f"{len(expected) - len(short)} of {len(expected)} folders fully in RAW")
    for folder, (want, got) in sorted(short.items()):
        print(f"  {folder}: logged {want:,}, in RAW {got:,}")
    return 0 if not short else 1


def check_raw() -> int:
    """Run snowflake/12_check_raw.sql; every query in it returns rows only when something is wrong."""
    import io

    from sf import connect
    from snowflake.connector.util_text import split_statements

    sql = (ROOT / "snowflake" / "12_check_raw.sql").read_text(encoding="utf-8")
    problems = 0
    conn = connect()
    try:
        cur = conn.cursor()
        for statement, _ in split_statements(io.StringIO(sql), remove_comments=True):
            cur.execute(statement)
            if cur.description and statement.lstrip().upper().startswith("SELECT"):
                rows = cur.fetchall()
                problems += len(rows)
                for row in rows[:5]:
                    print(f"  PROBLEM {row}")
    finally:
        conn.close()
    print(f"12_check_raw: {problems} problem row(s)")
    return 0 if problems == 0 else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="step", required=True)
    sub.add_parser("nights")
    p = sub.add_parser("land")
    p.add_argument("--night", type=date.fromisoformat, required=True)
    p = sub.add_parser("job")
    p.add_argument("name")
    p = sub.add_parser("raw-has")
    p.add_argument("summary")
    sub.add_parser("check-raw")
    args = parser.parse_args(argv)

    load_local_env(ROOT / ".env")
    if args.step == "nights":
        for i, d in enumerate(night_dates(), start=1):
            print(f"night {i}: {d}")
        return 0
    if args.step == "land":
        return land(args.night)
    if args.step == "job":
        return run_job(args.name)
    if args.step == "raw-has":
        return raw_has(args.summary)
    return check_raw()


if __name__ == "__main__":
    sys.exit(main())
