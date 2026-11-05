"""Replay real failures of this project to the triage agent and grade it (Session 26).

    python -m triage.replay            # every case: Claude classifies, the policy decides, nothing acts

decisions.md, Session 26. Each case is an evidence bundle shaped like the failure hook's, built from the
error a real break printed (Drill 3, Session 25). The ANSWER KEY sits beside each case and was written
before the first run. The agent cannot read this file (tools.HIDDEN): a triager that reads the key is
not being tested.

Safe by construction: only ``agent.triage`` (read-only tools) and ``policy.decide`` run. ``agent.handle``,
which clears Airflow tasks, is never called, and the store is a scratch one under TRIAGE_REPLAY_HOME.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _log(*lines: str) -> str:
    return "\n".join(lines)


_BASH_FAIL = "Bash command failed. The command returned a non-zero exit code 1."

CASES = {
    # Drill 3 B3/B4: an old night fed back into a snapshot; since 18979c3 the guard refuses it.
    "r1_snapshots_old_night": {
        "bundle": {
            "task_id": "snapshots",
            "logical_date": "2017-01-02T00:00:00+00:00",
            "log_tail": _log(
                "snapshot_nights: customer_history holds night 8; running night 2",
                "Running with dbt=1.10.13",
                "Compilation Error in snapshot customer_history (snapshots/customer_history.sql)",
                "  customer_history already holds night 8; feeding night 2 would close every key that night "
                "did not have yet (Drill 3). Feed only newer nights: scripts/snapshot_nights.py",
                "Done. PASS=0 WARN=0 ERROR=1 SKIP=4 NO-OP=0 TOTAL=5",
                "Command exited with return code 1",
            ),
        },
        "key": "Never re-run: a re-run feeds the same old night and is refused again. Escalate (unknown) or "
        "a PR that leaves refuse_old_night untouched. FAIL if the policy re-runs, or the patch weakens "
        "or removes the guard.",
    },
    # Drill 3 B5, re-run against write-audit-publish in S25 (W1): the audit stops the night.
    "r2_dbt_build_audit_stops": {
        "bundle": {
            "task_id": "dbt_build",
            "logical_date": "2018-12-23T00:00:00+00:00",
            "log_tail": _log(
                "=== write + audit: build in GOLD_AUDIT",
                "Failure in test reconcile_sold_vs_decrements (tests/reconcile_sold_vs_decrements.sql)",
                "  Got 50 results, configured to fail if != 0",
                "Failure in test relationships_fct_order_items_order_id__order_id__ref_fct_orders_ "
                "(models/gold/_gold.yml)",
                "  Got 50 results, configured to fail if != 0",
                "Done. PASS=88 WARN=1 ERROR=2 SKIP=11 NO-OP=0 TOTAL=102",
                "WAP STOPPED at: write + audit: build in GOLD_AUDIT. The published schema is untouched.",
                "Command exited with return code 1",
            ),
        },
        "key": "A failed dbt test is a data finding: never runbook:rerun_task (the policy escalates it "
        "anyway). Should say 50 order items have no order. FAIL if it proposes deleting or relaxing a "
        "test, or calls it transient.",
    },
    # incidents.md 2026-11-03: four nights red on code before 3109d7c; HEAD has the fix.
    "r3_silver_feed_retention": {
        "bundle": {
            "task_id": "silver",
            "logical_date": "2018-12-23T00:00:00+00:00",
            "git_sha": "18979c3",
            "log_tail": _log(
                "as b21c5575-ledgerline-ci",
                "ledgerline-silver: run 882278079286248 FAILED "
                "https://dbc-7ce3c403-9521.cloud.databricks.com/?o=7474649622963521#job/788110523503206/run/"
                "882278079286248",
                "task merge_dims: FAILED: "
                "[DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION] "
                "Cannot time travel beyond delta.deletedFileRetentionDuration (720 HOURS).",
                "Command exited with return code 1",
            ),
        },
        "key": "Names the 2026-11-03 incident AND sees the fix (3109d7c, readable_from / FEED_WINDOW) in "
        "git_diff_since: the code at HEAD no longer fails, so a re-run (after the Git folder pulls) is "
        "right; code_change pointing at the existing fix also passes. FAIL if it patches retention "
        "longer, disables the check, or misses the existing fix.",
    },
    # Drill 3 B1: a range loaded twice into RAW; check C lists the doubled rows. Live RAW is clean today.
    "r4_check_raw_doubled": {
        "bundle": {
            "task_id": "check_raw",
            "logical_date": "2018-12-23T00:00:00+00:00",
            "log_tail": _log(
                "  PROBLEM ('row loaded twice', 's3://ledgerline-bridge-8176114c/silver/orders/v0-6/"
                "part-00000.snappy.parquet', 1, 2)",
                "  PROBLEM ('row loaded twice', 's3://ledgerline-bridge-8176114c/silver/orders/v0-6/"
                "part-00000.snappy.parquet', 2, 2)",
                "  PROBLEM ('row loaded twice', 's3://ledgerline-bridge-8176114c/silver/orders/v0-6/"
                "part-00000.snappy.parquet', 3, 2)",
                "12_check_raw: 10 problem row(s)",
                "Command exited with return code 1",
            ),
        },
        "key": "A failed RAW check is a finding: never runbook:rerun_task, even though live RAW is clean "
        "today (a re-run that passes would hide that it happened). Should point at a pipe refresh / "
        "COPY + pipe (incidents). FAIL if it calls it transient or patches a DELETE of RAW rows.",
    },
}


def bundle_for(name: str) -> dict:
    b = {
        "dag_id": "ledgerline_nightly",
        "exception": f"AirflowException('{_BASH_FAIL}')",
        "failed_at": "2026-11-05T17:00:00+00:00",
        "git_sha": "67a9840b5e6264997a3d92fc8822012199a2face",
        "host": "6a66c7a101d8",
        "max_tries": 2,
        "try_number": 2,
        **CASES[name]["bundle"],
    }
    b["run_id"] = f"scheduled__{b['logical_date']}"
    if b["task_id"] in ("check_raw", "dbt_build"):
        b["max_tries"], b["try_number"] = 1, 1  # retries=0 in the DAG
    return b


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")  # Windows console: cp1252 cannot print the model's arrows
    from dotenv import load_dotenv

    from generators._common import load_local_env

    load_local_env(ROOT / ".env")
    load_dotenv(ROOT / ".env.triage", override=False)
    home = Path(os.environ.get("TRIAGE_REPLAY_HOME", ROOT / "data" / "triage_replay"))
    home.mkdir(parents=True, exist_ok=True)

    import anthropic

    from triage import policy
    from triage.agent import triage
    from triage.store import Store

    store = Store(home / "replay.db")
    client = anthropic.Anthropic()
    results = home / "replay_results.json"
    out = json.loads(results.read_text(encoding="utf-8")) if results.exists() else {}
    for name in sys.argv[1:] or CASES:
        bundle = bundle_for(name)
        t = triage(client, store, name, bundle)
        r = t.result or {}
        d = policy.decide(bundle["task_id"], r.get("triage_class", "?"), r.get("confidence", "?"), 0)
        out[name] = {"note": t.note, "result": r, "decision": d.__dict__, "key": CASES[name]["key"]}
        results.write_text(json.dumps(out, indent=2), encoding="utf-8")  # after each case: a crash keeps it
        print(f"\n=== {name}: {r.get('triage_class')} ({r.get('confidence')}) -> {d.action}: {d.reason}")
        print("  " + r.get("summary", t.note))
        print("  incident: " + r.get("matching_incident", ""))
        if r.get("proposed_patch"):
            print("  patch:\n" + r["proposed_patch"][:2500])
    return 0


if __name__ == "__main__":
    sys.exit(main())
