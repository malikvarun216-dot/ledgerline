"""End-to-end smoke tests for the three generator CLIs.

Why these exist
---------------
Every other test calls the generators as library functions. That leaves the
argparse wiring, the default flag values and the order of operations inside
``main``/``run`` completely uncovered — and the first manual end-to-end run of
``dim_dumps`` surfaced two real bugs that the library-level tests all passed
through (docs/incidents.md [2026-09-19], both entries).

So the command-line path gets exercised here, as a subprocess, the way a person
would actually invoke it.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    """Invoke a generator as a subprocess. ``python``, not ``python3`` (Windows)."""
    return subprocess.run(
        [sys.executable, *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )


@pytest.fixture
def streams_dir(tmp_path) -> Path:
    return tmp_path / "streams"


def test_order_events_cli_writes_a_keyed_jsonl_topic(raw_dir, streams_dir):
    result = run_cli(
        "generators/order_events.py",
        "--raw-dir", str(raw_dir),
        "--out-dir", str(streams_dir),
    )
    assert result.returncode == 0, result.stderr

    topic_file = streams_dir / "orders.jsonl"
    assert topic_file.exists()

    import json

    records = [json.loads(line) for line in topic_file.read_text(encoding="utf-8").splitlines()]
    assert len(records) == 23
    assert all(r["key"] == r["value"]["order_id"] for r in records), "key must be order_id"
    assert [r["value"]["event_ts"] for r in records] == sorted(
        r["value"]["event_ts"] for r in records
    ), "the topic file must be in event-time order"


def test_inventory_cdc_cli_reconciles_and_exits_zero(raw_dir, streams_dir):
    """The CLI returns non-zero if the tie-out fails, so exit code is the assertion."""
    result = run_cli(
        "generators/inventory_cdc.py",
        "--raw-dir", str(raw_dir),
        "--out-dir", str(streams_dir),
        "--delist-count", "1",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "units sold rebuilt from CDC deltas        9" in result.stdout
    assert (streams_dir / "inventory.cdc.jsonl").exists()


def test_inventory_cdc_cli_survives_out_of_order_emission(raw_dir, streams_dir):
    """Disturbed arrival order must not disturb the reconciliation."""
    result = run_cli(
        "generators/inventory_cdc.py",
        "--raw-dir", str(raw_dir),
        "--out-dir", str(streams_dir),
        "--out-of-order-pct", "40",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "seq values are still correct" in result.stdout


def test_dim_dumps_cli_grows_the_customer_dimension(raw_dir, tmp_path):
    """Regression guard for the drained-dimension incident, at the CLI level."""
    out_dir = tmp_path / "dumps"
    result = run_cli(
        "generators/dim_dumps.py",
        "--raw-dir", str(raw_dir),
        "--out-dir", str(out_dir),
        "--start-date", "2017-01-31",
        "--nights", "5",
        "--stride-days", "28",
        "--change-rate", "1",
        "--delete-per-night", "1",
    )
    assert result.returncode == 0, result.stderr

    counts = []
    for night in range(5):
        dump_date = (pd.Timestamp("2017-01-31") + pd.Timedelta(days=28 * night)).date()
        path = out_dir / "dims" / "customer" / f"dump_date={dump_date}" / "customer.csv"
        assert path.exists(), f"missing dump for {dump_date}"
        counts.append(len(pd.read_csv(path)))

    assert min(counts) > 0, f"a dimension emptied: {counts}"
    assert counts == sorted(counts), f"the customer dimension shrank: {counts}"


def test_generators_refuse_to_run_without_the_raw_data(tmp_path, streams_dir):
    """A missing download must name the fix, not raise a bare FileNotFoundError."""
    result = run_cli(
        "generators/order_events.py",
        "--raw-dir", str(tmp_path / "nothing-here"),
        "--out-dir", str(streams_dir),
    )
    assert result.returncode != 0
    assert "fetch_olist" in result.stderr


def test_print_schema_emits_valid_avro(streams_dir):
    """The Avro schemas must be parseable before Session 3 points them at a registry."""
    import json

    for module, record_name in (
        ("generators/order_events.py", "OrderEvent"),
        ("generators/inventory_cdc.py", "InventoryChange"),
    ):
        result = run_cli(module, "--print-schema")
        assert result.returncode == 0, result.stderr
        schema = json.loads(result.stdout)
        assert schema["type"] == "record"
        assert schema["name"] == record_name
        field_names = [f["name"] for f in schema["fields"]]
        # the shared envelope must be present on every topic
        assert {"event_id", "event_ts", "produced_at", "source", "schema_version"} <= set(field_names)


def test_fetch_olist_check_only_reports_without_downloading(tmp_path):
    """--check-only must never touch the network or ask for a credential."""
    result = run_cli("scripts/fetch_olist.py", "--check-only", "--raw-dir", str(tmp_path))
    assert result.returncode == 1, "an empty raw dir is a failure"
    assert "MISSING" in result.stdout
    assert "kaggle.json" not in result.stdout, "check-only must not prompt for a token"
