"""Session 21: files read vs pruned for one ship date, on each of the four layouts — on the SQL warehouse.

    python experiments/s21_skipping_on_warehouse.py

Why a second script: the notebook (`s21_spark_performance`) built the four copies, but its queries
are not in `system.query.history` with their SQL text and the Query History API did not return them
(constraints.md, S21). A query sent to the SQL warehouse is recorded with its profile's metrics, so
the same one-day query is sent there, as the `ledgerline-ci` service principal (needs USE SCHEMA +
SELECT on `workspace.s21`, granted for S21), and the metrics are read back by statement id.

Predictions were written before the first run; a wrong one is the finding.
"""

from __future__ import annotations

import os
import sys
import time
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from query_as import api, token  # noqa: E402

from generators._common import load_local_env  # noqa: E402

S = "workspace.s21"
DAY = "1995-03-15"
SUM = "476268129.98"  # the notebook's answer for that day, on every layout
FILES = {"li_random": 34, "li_part": 2526, "li_zorder": 2, "li_liquid": 15}  # DESCRIBE DETAIL, S21

results: list[tuple[str, object, object, bool]] = []


def check(label, got, predicted, ok=None):
    ok = (got == predicted) if ok is None else ok
    results.append((label, got, predicted, ok))
    print(f"  {'ok   ' if ok else 'WRONG'} {label}: got {got!r}, predicted {predicted!r}")


def statement(host: str, bearer: str, warehouse: str, sql: str) -> tuple[str, str]:
    """Run one statement; (statement id, first cell of the result). A failure stops the run."""
    r = api(host, bearer, "POST", "/api/2.0/sql/statements",
            {"warehouse_id": warehouse, "statement": sql, "wait_timeout": "50s"})
    state = r.get("status", {}).get("state")
    if state != "SUCCEEDED":
        message = r.get("status", {}).get("error", {}).get("message", "")
        raise SystemExit(f"{state}: {message[:300]}")  # a failed query's metrics are zeros, not a finding
    rows = r.get("result", {}).get("data_array") or [[None]]
    return r["statement_id"], rows[0][0]


def metrics(host: str, bearer: str, statement_id: str, timeout: int = 180) -> dict | None:
    """The query profile's numbers for one statement, from the Query History API."""
    query = urllib.parse.urlencode({"filter_by.statement_ids": statement_id, "include_metrics": "true"})
    end = time.time() + timeout
    while time.time() < end:
        for q in api(host, bearer, "GET", f"/api/2.0/sql/history/queries?{query}").get("res", []):
            m = q.get("metrics") or {}
            if "read_files_count" in m or "pruned_files_count" in m:
                return {"read": m.get("read_files_count", 0), "pruned": m.get("pruned_files_count", 0),
                        "read_mb": round(m.get("read_bytes", 0) / 2**20, 1), "ms": m.get("total_time_ms"),
                        "cache": m.get("result_from_cache")}
        time.sleep(10)
    return None


def main() -> int:
    load_local_env()
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    bearer = token(host, os.environ["DATABRICKS_CLIENT_ID"], os.environ["DATABRICKS_CLIENT_SECRET"])
    warehouse = api(host, bearer, "GET", "/api/2.0/sql/warehouses")["warehouses"][0]["id"]
    run = int(time.time())  # a new text each run, so the result cache cannot answer
    seen = {}
    for name in FILES:
        sid, answer = statement(host, bearer, warehouse,
                                f"SELECT /* s21-wh-{name}-{run} */ round(sum(l_extendedprice), 2) "
                                f"FROM {S}.{name} WHERE l_shipdate = '{DAY}'")
        seen[name] = {"answer": answer, "metrics": metrics(host, bearer, sid)}
        print(f"  {name} ({FILES[name]} files): {seen[name]}")
    check("W0 every layout answers the notebook's sum", {v["answer"] for v in seen.values()}, {SUM})
    if not all(v["metrics"] for v in seen.values()):
        print("  metrics missing for some layout: read them in Query History (warehouse queries) by hand")
        return 0
    m = {k: v["metrics"] for k, v in seen.items()}
    check("W1 li_random: reads >= 90% of its 34 files", m["li_random"]["read"] >= 0.9 * 34, True)
    check("W2 li_part: reads 1 of 2,526 (the date's own folder)", m["li_part"]["read"], 1)
    check("W3 li_zorder: reads <= 2 of 2", m["li_zorder"]["read"] <= 2, True)
    check("W4 li_liquid: reads <= 2 of 15", m["li_liquid"]["read"] <= 2, True)
    wrong = [r for r in results if not r[3]]
    print(f"\n{len(results) - len(wrong)}/{len(results)} as predicted")
    for label, got, predicted, _ in wrong:
        print(f"  WRONG {label}: got {got!r}, predicted {predicted!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
