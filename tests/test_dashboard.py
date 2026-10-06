"""The pipeline-health dashboard file must be exactly what its SQL builds.

The dashboard's lag and held-rows tiles run the same SQL files as the two SQL Alerts. If an alert query
is edited and the dashboard file is not rebuilt, the page and the alarm disagree — the page would show
a healthy 0 from yesterday's logic. This test makes that a red suite instead of a quiet drift.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "databricks" / "dashboards" / "pipeline_health.lvdash.json"


def test_dashboard_file_matches_its_sql():
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "build_dashboard.py"), "--check"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_every_widget_reads_a_dataset_that_exists():
    doc = json.loads(OUT.read_text(encoding="utf-8"))
    datasets = {d["name"] for d in doc["datasets"]}
    used = {
        q["query"]["datasetName"]
        for page in doc["pages"] for item in page["layout"]
        for q in item["widget"].get("queries", [])
    }
    assert used <= datasets, f"widgets read datasets that do not exist: {used - datasets}"
    assert datasets <= used, f"datasets no widget reads: {datasets - used}"
