"""Did every scheduled Databricks job succeed recently? A check that lives OUTSIDE Databricks.

    python scripts/watch_jobs.py                 # exit 0 = all fresh, 1 = a job is stale, 2 = could not check
    python scripts/watch_jobs.py --max-age-hours 26

incidents.md 2026-10-18: Free Edition did not start either scheduled job on 4 of 17 days (10-11,
10-16, 10-17, 10-18) and said nothing. Every alarm we had lives inside Databricks (job-failure
emails, SQL Alerts), and a run that never starts is not a failure: nothing fires. So this asks
from the outside, through the Jobs API, "when did each scheduled job last succeed?". Run daily by
`.github/workflows/job_watch.yml`. A failed scheduled workflow is emailed by GitHub.

Watches every job whose schedule is UNPAUSED (a job with no schedule, like `ledgerline-bridge`,
is started by Airflow and is not expected daily). Signs in as the `ledgerline-ci` service principal
(DATABRICKS_HOST / CLIENT_ID / CLIENT_SECRET), read-only: it lists jobs and runs, starts nothing.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from generators._common import load_local_env  # noqa: E402

# Daily jobs at 06:00 / 06:30 IST (00:30 / 01:00 UTC); the workflow checks at 03:30 UTC. A day's
# run that never started leaves the newest success at least 26.5 h old by then; a normal day, ~3 h.
DEFAULT_MAX_AGE = timedelta(hours=26)


@dataclass(frozen=True)
class JobStatus:
    name: str
    last_success: datetime | None  # end of the newest successful run; None = none found


def stale(jobs: list[JobStatus], now: datetime, max_age: timedelta) -> list[JobStatus]:
    """Jobs whose newest success is older than ``max_age`` (or that have none)."""
    return [j for j in jobs if j.last_success is None or now - j.last_success > max_age]


def _ms(ts: int | None) -> datetime | None:
    return datetime.fromtimestamp(ts / 1000, UTC) if ts else None


def scheduled_job_statuses() -> list[JobStatus]:
    """Every job with an UNPAUSED schedule, with the end time of its newest successful run."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import PauseStatus, RunResultState

    # Explicit OAuth M2M: .env also holds a personal DATABRICKS_TOKEN, and two auth methods at once
    # is an error in the SDK. The watcher must not act as a person anyway.
    w = WorkspaceClient(
        host=os.environ["DATABRICKS_HOST"],
        client_id=os.environ["DATABRICKS_CLIENT_ID"],
        client_secret=os.environ["DATABRICKS_CLIENT_SECRET"],
        auth_type="oauth-m2m",
    )
    out = []
    for job in w.jobs.list():
        schedule = job.settings.schedule if job.settings else None
        if not schedule or schedule.pause_status != PauseStatus.UNPAUSED:
            continue
        last = None
        # Newest first, completed only; 25 runs is weeks of daily runs plus manual ones.
        for run in w.jobs.list_runs(job_id=job.job_id, completed_only=True, limit=25):
            if run.state and run.state.result_state == RunResultState.SUCCESS:
                last = _ms(run.end_time)
                break
        out.append(JobStatus(job.settings.name, last))
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--max-age-hours", type=float, default=DEFAULT_MAX_AGE.total_seconds() / 3600)
    args = parser.parse_args(argv)
    max_age = timedelta(hours=args.max_age_hours)

    load_local_env(ROOT / ".env")
    try:
        jobs = scheduled_job_statuses()
    except Exception as exc:  # a watcher that cannot look must say so, loudly
        print(f"COULD NOT CHECK: {type(exc).__name__}: {exc}")
        return 2
    if not jobs:
        print("COULD NOT CHECK: no job with an active schedule is visible to this identity")
        return 2

    now = datetime.now(UTC)
    late = stale(jobs, now, max_age)
    print(f"now {now:%Y-%m-%d %H:%M} UTC, max age {max_age.total_seconds() / 3600:g} h")
    for j in jobs:
        age = f"{(now - j.last_success).total_seconds() / 3600:.1f} h ago" if j.last_success else "never"
        flag = "STALE" if j in late else "ok"
        print(f"  {flag:5}  {j.name}: last success {age}")
    if late:
        print(
            f"{len(late)} job(s) without a successful run in {max_age.total_seconds() / 3600:g} h. "
            "A missed day is a finding: read the job's Runs page (incidents.md 2026-10-18)."
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
