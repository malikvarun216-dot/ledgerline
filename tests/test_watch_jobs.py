"""scripts/watch_jobs.py — the staleness rule, without Databricks (incidents.md 2026-10-18)."""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from watch_jobs import DEFAULT_MAX_AGE, JobStatus, main, stale

# The workflow's check time: 03:30 UTC (09:00 IST).
NOW = datetime(2026, 10, 18, 3, 30, tzinfo=UTC)


def test_todays_run_is_fresh():
    silver = JobStatus("ledgerline-silver", datetime(2026, 10, 18, 0, 38, tzinfo=UTC))
    assert stale([silver], NOW, DEFAULT_MAX_AGE) == []


def test_a_missed_day_is_stale_for_both_schedules():
    # 10-18 never started: the newest successes are yesterday's, 26.9 h and 26.4 h old.
    silver = JobStatus("ledgerline-silver", datetime(2026, 10, 17, 0, 38, tzinfo=UTC))
    pii = JobStatus("ledgerline-pii", datetime(2026, 10, 17, 1, 5, tzinfo=UTC))
    assert stale([silver, pii], NOW, DEFAULT_MAX_AGE) == [silver, pii]


def test_no_success_at_all_is_stale():
    assert stale([JobStatus("new-job", None)], NOW, timedelta(hours=26)) == [JobStatus("new-job", None)]


def test_cannot_check_is_a_failure():
    # conftest's no_live_services removes the credentials: the watcher must exit 2, never 0.
    assert main([]) == 2
