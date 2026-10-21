"""What the triage agent may do on its own — decided here, in code, never by the model (Session 19c).

decisions.md, Session 19c. Claude proposes a class; this module decides. A re-run is allowed only for
a task that is safe to repeat (every step of ledgerline_nightly is — that is what makes Airflow's own
retries safe) AND whose failure is not a finding by design. ``check_raw`` and ``dbt_build`` run with
``retries=0`` in the DAG for exactly that reason: "a data problem does not fix itself on a retry".
Re-running them would turn a red check into a green one by luck.

Never, whatever the class: anything that deletes or regenerates data, touches governance or PII, or
changes a MERGE. Those are not actions this agent has; they reach a human as an escalation.
"""

from __future__ import annotations

from dataclasses import dataclass

MAX_RERUNS = 2

# task_id -> why a re-run is (or is not) a safe repair. Every task of ledgerline_nightly is listed:
# a task added to the DAG and not here is escalated, never re-run by default.
RERUNNABLE = {
    "land_night": "write-once landing: an already landed night is compared and left alone",
    "silver": "Databricks job idempotent by its merge log",
    "bridge_export": "idempotent by its export log; a re-run exports nothing new",
    "wait_for_raw": "read only: waits for Snowpipe",
    "snapshots": "snapshot_nights.py runs only the nights a snapshot does not hold yet",
}
FINDINGS = {
    "check_raw": "retries=0 in the DAG: a failed RAW check is a data finding, not a flake",
    "dbt_build": "retries=0 in the DAG: a failed dbt test is a finding, not a flake",
}
CLASSES = ("runbook:rerun_task", "code_change", "unknown")


@dataclass(frozen=True)
class Decision:
    action: str  # rerun / propose_pr / escalate
    reason: str
    attempt: int = 0  # for rerun: which attempt this is (1..MAX_RERUNS)


def decide(task_id: str, triage_class: str, confidence: str, reruns_done: int) -> Decision:
    if triage_class not in CLASSES:
        return Decision("escalate", f"unknown class {triage_class!r} from the model")
    if triage_class == "code_change":
        return Decision("propose_pr", "code changes go through a pull request a human merges")
    if triage_class == "unknown":
        return Decision("escalate", "the model could not name the failure")
    # runbook:rerun_task
    if task_id in FINDINGS:
        return Decision("escalate", f"{task_id}: {FINDINGS[task_id]}")
    if task_id not in RERUNNABLE:
        return Decision("escalate", f"{task_id} is not on the re-runnable list")
    if confidence != "high":
        return Decision("escalate", f"re-run proposed with {confidence} confidence; only 'high' acts alone")
    if reruns_done >= MAX_RERUNS:
        return Decision(
            "escalate", f"{reruns_done} re-runs already spent on this task tonight (max {MAX_RERUNS})"
        )
    return Decision("rerun", f"{task_id}: {RERUNNABLE[task_id]}", attempt=reruns_done + 1)
