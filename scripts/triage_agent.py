"""Run the triage agent over the failure inbox (Session 19c; decisions.md, Session 19c).

    python scripts/triage_agent.py              # one pass: new evidence files + unfinished incidents
    python scripts/triage_agent.py --status     # what the agent holds: incidents, receipts, AI spend

Reads data/triage/inbox/ (written by Airflow's failure hook), keeps its state in
data/triage/triage.db, writes proposals and escalations next to them. Needs Airflow up on
localhost:8080, the Snowflake CI key for its read-only queries, and ANTHROPIC_API_KEY in
``.env.triage`` — NOT ``.env``: Airflow's containers load all of ``.env`` (docker-compose env_file),
and no task needs the agent's key. ``.env.*`` is gitignored.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from generators._common import load_local_env  # noqa: E402


def status(store) -> None:
    for title, sql in (
        (
            "incidents",
            "SELECT bundle, status, triage_class, json_extract(decision, '$.action') AS action,"
            " opened_at, closed_at FROM incidents ORDER BY opened_at",
        ),
        (
            "receipts",
            "SELECT dag_run_id, task_id, action, attempt, status, result FROM receipts ORDER BY started_at",
        ),
        (
            "AI spend",
            "SELECT model, COUNT(*) AS calls, SUM(input_tokens) AS input_tokens,"
            " SUM(output_tokens) AS output_tokens, ROUND(SUM(cost_usd), 4) AS usd"
            " FROM ai_calls GROUP BY model",
        ),
    ):
        rows = store.db.execute(sql).fetchall()
        print(f"\n{title}: {len(rows)}")
        for r in rows:
            print("  " + " | ".join(str(v) for v in tuple(r)))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args(argv)
    load_local_env(ROOT / ".env")
    if not os.environ.get("LEDGERLINE_NO_DOTENV"):  # tests never read a real key (conftest)
        from dotenv import load_dotenv

        load_dotenv(ROOT / ".env.triage", override=False)

    from triage.agent import HOME, run_once
    from triage.store import Store

    HOME.mkdir(parents=True, exist_ok=True)
    store = Store(HOME / "triage.db")
    if args.status:
        status(store)
        return 0

    import anthropic

    from triage.airflow_api import Airflow

    outcome = run_once(store, anthropic.Anthropic(), Airflow())
    print(f"\n{len(outcome)} incident(s) worked: " + ", ".join(f"{s}" for s in outcome.values()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
