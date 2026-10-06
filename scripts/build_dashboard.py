"""Build the pipeline-health AI/BI dashboard file from the SQL kept in git.

An AI/BI dashboard (formerly "Lakeview") is stored by Databricks as one JSON document: its datasets
(SQL queries) and its pages of widgets. Writing that JSON by hand would copy every query a second
time, and the copy would drift from the alert SQL it repeats. So the queries live as .sql files —
the two alert queries in databricks/alerts/, the rest in databricks/dashboards/pipeline_health/ —
and this script assembles them into databricks/dashboards/pipeline_health.lvdash.json.

    python scripts/build_dashboard.py           # write the file
    python scripts/build_dashboard.py --check   # exit 1 if the file is not what the SQL builds

Import it in Databricks: Dashboards -> Create (the arrow) -> Import dashboard from file. tests/
test_dashboard.py runs --check, so a query edited without rebuilding fails the suite.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = ROOT / "databricks" / "dashboards" / "pipeline_health"
ALERTS = ROOT / "databricks" / "alerts"
OUT = ROOT / "databricks" / "dashboards" / "pipeline_health.lvdash.json"

# dataset name -> (title, SQL file)
DATASETS = {
    "lag": ("Silver behind Bronze (the alert)", ALERTS / "silver_behind_bronze.sql"),
    "held": ("Dims rows held (the alert)", ALERTS / "dims_rows_held.sql"),
    "reconciliation": ("Units ordered vs decremented, per SKU", SQL_DIR / "reconciliation.sql"),
    "quota": ("Tables per schema (UC quota 100)", SQL_DIR / "quota.sql"),
    "freshness": ("Bronze landed vs Silver applied", SQL_DIR / "freshness.sql"),
    "silver_daily": ("Silver rows applied per day", SQL_DIR / "silver_daily.sql"),
    "silver_runs": ("Silver merge logs", SQL_DIR / "silver_runs.sql"),
    "expectations": ("Lakeflow expectations", SQL_DIR / "lakeflow_expectations.sql"),
    "bronze_progress": ("Kafka Bronze micro-batches", SQL_DIR / "bronze_progress.sql"),
}

INTRO = """## ledgerline — pipeline health
Every tile is a query on the production tables; **0 is healthy** on the four numbers. The lag and
held-rows tiles run the **same SQL as the two SQL Alerts** (`databricks/alerts/`), so the page and the
alarms cannot disagree. Built from git by `scripts/build_dashboard.py` (Session 11)."""


def _dataset(name: str, title: str, path: Path) -> dict:
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    return {"name": name, "displayName": title, "queryLines": lines}


def _query(dataset: str, columns: list[str]) -> list[dict]:
    fields = [{"name": c, "expression": f"`{c}`"} for c in columns]
    return [{"name": "main_query",
             "query": {"datasetName": dataset, "fields": fields, "disaggregated": True}}]


def _frame(title: str, description: str = "") -> dict:
    frame = {"showTitle": True, "title": title}
    if description:
        frame.update(showDescription=True, description=description)
    return frame


def counter(name, dataset, column, title, description, position):
    return {
        "widget": {
            "name": name,
            "queries": _query(dataset, [column]),
            "spec": {
                "version": 2,
                "widgetType": "counter",
                "encodings": {"value": {"fieldName": column, "displayName": column}},
                "frame": _frame(title, description),
            },
        },
        "position": position,
    }


def _table_column(field: str, order: int) -> dict:
    return {
        "fieldName": field, "booleanValues": ["false", "true"], "imageUrlTemplate": "{{ @ }}",
        "imageTitleTemplate": "{{ @ }}", "imageWidth": "", "imageHeight": "",
        "linkUrlTemplate": "{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}",
        "linkOpenInNewTab": True, "type": "string", "displayAs": "string", "visible": True,
        "order": order, "title": field, "allowSearch": False, "alignContent": "left",
        "allowHTML": False, "highlightLinks": False, "useMonospaceFont": False,
        "preserveWhitespace": False, "displayName": field,
    }


def table(name, dataset, columns, title, description, position):
    return {
        "widget": {
            "name": name,
            "queries": _query(dataset, columns),
            "spec": {
                "version": 1,
                "widgetType": "table",
                "encodings": {"columns": [_table_column(c, 100000 + i) for i, c in enumerate(columns)]},
                "invisibleColumns": [],
                "allowHTMLByDefault": False,
                "itemsPerPage": 25,
                "paginationSize": "default",
                "condensed": True,
                "withRowNumber": False,
                "frame": _frame(title, description),
            },
        },
        "position": position,
    }


def bar(name, dataset, x, y, title, description, position, *, color=None, x_scale="categorical"):
    encodings = {
        "x": {"fieldName": x, "scale": {"type": x_scale}, "displayName": x},
        "y": {"fieldName": y, "scale": {"type": "quantitative"}, "displayName": y},
    }
    columns = [x, y]
    if color:
        encodings["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color}
        columns.append(color)
    return {
        "widget": {
            "name": name,
            "queries": _query(dataset, columns),
            "spec": {"version": 3, "widgetType": "bar", "encodings": encodings,
                     "frame": _frame(title, description)},
        },
        "position": position,
    }


def text(name, markdown, position):
    return {"widget": {"name": name, "textbox_spec": markdown}, "position": position}


def at(x, y, width, height):
    return {"x": x, "y": y, "width": width, "height": height}


def build() -> dict:
    layout = [
        text("intro", INTRO, at(0, 0, 6, 2)),
        counter("c_lag", "lag", "sources_behind", "Sources behind",
                "Bronze data older than 26 h not yet in Silver", at(0, 2, 1, 3)),
        counter("c_held", "held", "rows_held", "Dims rows held", "Bad values kept at old values",
                at(1, 2, 1, 3)),
        counter("c_recon", "reconciliation", "skus_mismatched", "SKUs mismatched",
                "Units ordered vs stock decremented, per SKU", at(2, 2, 2, 3)),
        table("t_recon", "reconciliation",
              ["skus", "skus_mismatched", "units_ordered", "units_decremented"],
              "Reconciliation", "Expected 34,448 / 0 / 112,650 / 112,650", at(4, 2, 2, 3)),
        table("t_lag", "lag",
              ["sources_behind", "dims_behind", "old_nights_changed", "cdc_behind", "orders_behind"],
              "Which part of the lag alarm", "Each part of the alert query", at(0, 5, 3, 3)),
        table("t_fresh", "freshness", ["source", "bronze_last_landed", "silver_last_applied"],
              "Freshness", "Last data into Bronze, last data applied in Silver", at(3, 5, 3, 3)),
        bar("b_quota", "quota", "table_schema", "tables", "Tables per schema",
            "Unity Catalog allows 100 per schema; Databricks emails at 80", at(0, 8, 3, 6)),
        bar("b_daily", "silver_daily", "day", "rows_in", "Silver rows applied per day",
            "Bronze messages / file rows each stream applied", at(3, 8, 3, 6),
            color="stream", x_scale="temporal"),
        table("t_runs", "silver_runs",
              ["stream", "unit", "rows_in", "inserted", "updated", "deleted", "applied_at"],
              "Silver merge logs", "One line per micro-batch or night, newest first", at(0, 14, 6, 7)),
        table("t_expect", "expectations",
              ["update_started", "update_id", "dataset", "expectation", "passed", "failed"],
              "Lakeflow expectations (ledgerline-dlt)", "From the pipeline's event log", at(0, 21, 6, 7)),
        table("t_bronze", "bronze_progress",
              ["stream", "batch_id", "batch_started_at", "rows_by_offsets", "backlog_after_batch"],
              "Kafka Bronze micro-batches", "Messages per batch and backlog after it", at(0, 28, 6, 7)),
    ]
    return {
        "datasets": [_dataset(name, title, path) for name, (title, path) in DATASETS.items()],
        "pages": [{"name": "health", "displayName": "Pipeline health", "layout": layout,
                   "pageType": "PAGE_TYPE_CANVAS"}],
    }


def render() -> str:
    return json.dumps(build(), indent=2) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="fail if the file is stale")
    args = parser.parse_args()
    content = render()
    if args.check:
        current = OUT.read_text(encoding="utf-8") if OUT.exists() else ""
        if current != content:
            print(f"{OUT.relative_to(ROOT)} is stale: run python scripts/build_dashboard.py")
            return 1
        print("dashboard file matches its SQL")
        return 0
    OUT.write_text(content, encoding="utf-8", newline="\n")
    widgets = len(build()["pages"][0]["layout"])
    print(f"wrote {OUT.relative_to(ROOT)}: {len(DATASETS)} datasets, {widgets} widgets")
    return 0


if __name__ == "__main__":
    sys.exit(main())
