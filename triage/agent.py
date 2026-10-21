"""The triage agent: evidence file -> Claude's class -> policy -> act -> verify -> close (Session 19c).

decisions.md, Session 19c (two entries). One pass over the inbox:

    for each evidence file not yet taken in, and each incident left unfinished by a restart:
        1. triage   Claude reads the bundle with read-only tools, ends with submit_triage
        2. decide   policy.decide() — code, not the model — picks rerun / propose_pr / escalate
        3. act      rerun: receipt claimed BEFORE asking Airflow to clear the task, closed after
        4. verify   the night's DAG run succeeded (its dbt_build ran every dbt test, the
                    sold-vs-decrements reconciliation among them) AND the agent's own read-only
                    re-checks pass: 12_check_raw.sql and the reconciliation test
        5. close    only after 4; otherwise escalate to the human

A restarted agent never repeats a repair: an action is claimed by inserting its receipt, and an
existing receipt — even one left 'started' by a crash — is never fired again. The agent reads
Airflow's state instead and carries on from there.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from triage import policy
from triage.store import AlreadyClaimed, Store, now
from triage.tools import ROOT, run_tool, tool_specs

HOME = Path(os.environ.get("TRIAGE_HOME", ROOT / "data" / "triage"))
MODEL = os.environ.get("TRIAGE_MODEL", "claude-opus-5-5")
MAX_TURNS = 8
DAILY_CAP_USD = float(os.environ.get("TRIAGE_DAILY_USD", "5"))
# $ per million tokens: input, output, cache read, cache write (5-minute). Read 2026-10-22.
PRICES = {
    "claude-opus-5-5": (4.00, 20.00, 0.20, 5.00),
    "claude-opus-4-8": (5.00, 25.00, 0.50, 6.25),
    "claude-sonnet-5-5": (2.00, 10.00, 0.20, 2.50),
}

SYSTEM = """You triage a failed task of a nightly data pipeline (Airflow DAG `ledgerline_nightly`).
One DAG run is one "dims night". Tasks, in order:
  land_night (S3 landing, write-once) -> silver (Databricks job) -> bridge_export (Databricks job, writes
  Parquet for Snowflake) -> wait_for_raw (sensor: Snowpipe loads RAW) -> check_raw (12_check_raw.sql; a
  failure is a data finding) -> snapshots (dbt snapshots, SCD2) -> dbt_build (Gold models + every dbt test).
Airflow already retried the task once before you see it.

Classify the failure into exactly one class:
- runbook:rerun_task — transient: network, timeout, a platform hiccup, an expired session, a job that
  failed for a reason outside the repository and the data. Re-running the same code on the same data
  would succeed.
- code_change — the repository is wrong: a bug, a model or SQL that does not match the data, a broken
  script. Re-running would fail again. Give the smallest patch that fixes it, as a unified diff
  against HEAD, and a pull-request title and body.
- unknown — the evidence does not support either; say what is missing.

Rules:
- Read the evidence before deciding. docs/incidents.md lists every known failure mode of this project
  with its prevention rule: search it for the error text. git_diff_since(bundle git_sha) shows what
  changed since the task's code version.
- A failing data check (check_raw, a dbt test) is not transient: never call it runbook:rerun_task
  unless the evidence shows the check itself could not run (e.g. a connection error).
- confidence "high" only when a specific line of evidence proves the class.
- You only read. What happens next is decided by written policy, not by you.
- Finish by calling submit_triage exactly once. Cite evidence as short quotes with their source."""

SUBMIT = {
    "name": "submit_triage",
    "description": "Your final classification. Call exactly once, when done reading.",
    "strict": True,
    "input_schema": {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "triage_class",
            "confidence",
            "summary",
            "evidence",
            "matching_incident",
            "proposed_patch",
            "pr_title",
            "pr_body",
        ],
        "properties": {
            "triage_class": {"type": "string", "enum": list(policy.CLASSES)},
            "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
            "summary": {"type": "string", "description": "two sentences, plain words"},
            "evidence": {"type": "array", "items": {"type": "string"}},
            "matching_incident": {
                "type": "string",
                "description": "heading of the matching docs/incidents.md entry, or 'none'",
            },
            "proposed_patch": {"type": "string", "description": "unified diff for code_change, else ''"},
            "pr_title": {"type": "string", "description": "for code_change, else ''"},
            "pr_body": {"type": "string", "description": "for code_change, else ''"},
        },
    },
}


def cost_usd(model: str, usage) -> float:
    p_in, p_out, p_read, p_write = PRICES.get(model, PRICES["claude-opus-5-5"])
    return (
        usage.input_tokens * p_in
        + usage.output_tokens * p_out
        + (getattr(usage, "cache_read_input_tokens", 0) or 0) * p_read
        + (getattr(usage, "cache_creation_input_tokens", 0) or 0) * p_write
    ) / 1_000_000


@dataclass
class Triage:
    result: dict | None  # submit_triage's input, or None when the model never submitted
    note: str


def triage(client, store: Store, bundle_name: str, bundle: dict) -> Triage:
    """Claude with read-only tools, at most MAX_TURNS calls, every call's spend logged."""
    messages = [
        {
            "role": "user",
            "content": "Evidence bundle of the failed task:\n" + json.dumps(bundle, indent=2, sort_keys=True),
        }
    ]
    tools = [*tool_specs(), SUBMIT]
    nudged = False
    for turn in range(1, MAX_TURNS + 1):
        resp = client.beta.messages.create(
            model=MODEL,
            max_tokens=16000,
            system=SYSTEM,
            tools=tools,
            messages=messages,
            thinking={"type": "adaptive"},
            betas=["server-side-fallback-2026-07-01"],
            extra_body={"output_config": {"effort": "medium"}, "fallbacks": "default"},
        )
        store.log_call(
            bundle_name,
            resp.model,
            turn,
            resp.usage,
            cost_usd(resp.model, resp.usage),
            resp.stop_reason,
            getattr(resp, "_request_id", None),
        )
        if resp.stop_reason == "refusal":
            return Triage(None, "the model refused")
        messages.append({"role": "assistant", "content": resp.content})
        calls = [b for b in resp.content if b.type == "tool_use"]
        for b in calls:
            if b.name == "submit_triage":
                return Triage(dict(b.input), f"submitted on turn {turn}")
        if not calls:
            if nudged:
                return Triage(None, "the model stopped without submitting")
            nudged = True
            messages.append({"role": "user", "content": "Call submit_triage now with your classification."})
            continue
        results = []
        for b in calls:
            text, is_error = run_tool(b.name, dict(b.input))
            results.append(
                {"type": "tool_result", "tool_use_id": b.id, "content": text, "is_error": is_error}
            )
        messages.append({"role": "user", "content": results})
    return Triage(None, f"no submission within {MAX_TURNS} turns")


def run_check(*args: str) -> dict:
    out = subprocess.run(
        [sys.executable, *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=1800,
    )
    return {"command": " ".join(args), "exit": out.returncode, "tail": (out.stdout + out.stderr)[-1500:]}


def verify(airflow, bundle: dict) -> dict:
    """The incident closes only if the night's run succeeded and the agent's own re-checks pass."""
    state = airflow.run_state(bundle["dag_id"], bundle["run_id"])
    checks = {"dag_run_state": state}
    if state == "success":
        checks["check_raw"] = run_check("scripts/pipeline_step.py", "check-raw")
        checks["reconciliation"] = run_check(
            "scripts/run_dbt.py", "test", "--select", "reconcile_sold_vs_decrements"
        )
    checks["passed"] = state == "success" and all(
        checks[k]["exit"] == 0 for k in ("check_raw", "reconciliation")
    )
    return checks


def write_report(bundle_name: str, kind: str, text: str) -> Path:
    folder = HOME / kind
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / (bundle_name.removesuffix(".json") + (".patch" if kind == "proposals" else ".md"))
    path.write_text(text, encoding="utf-8", newline="\n")
    return path


def handle(store: Store, client, airflow, bundle_name: str, bundle: dict, log=print) -> str:
    """Take one incident as far as it can go. Returns its final status."""
    inc = store.incident(bundle_name)
    dag_run, task = bundle["run_id"], bundle["task_id"]

    # 1. triage — skipped on a restart if it already happened
    if inc["triage"] is None:
        if store.spend_usd(since=inc["opened_at"][:10]) >= DAILY_CAP_USD:
            store.update_incident(
                bundle_name,
                status="escalated",
                decision={"action": "escalate", "reason": f"daily AI spend cap ${DAILY_CAP_USD} reached"},
            )
            return "escalated"
        t = triage(client, store, bundle_name, bundle)
        if t.result is None:
            store.update_incident(
                bundle_name,
                status="escalated",
                triage=json.dumps({"note": t.note}),
                decision={"action": "escalate", "reason": t.note},
            )
            return "escalated"
        store.update_incident(bundle_name, triage=t.result, triage_class=t.result["triage_class"])
        inc = store.incident(bundle_name)
    result = json.loads(inc["triage"])
    log(f"  class {result['triage_class']} ({result['confidence']}): {result['summary']}")

    # 2. decide — receipts across ALL incidents of this task in this run bound the re-runs
    done = store.receipts(dag_run, task, "rerun")
    if inc["status"] == "acting" and done and done[-1]["status"] == "started":
        # A crash between claim and finish: never fire again; read what Airflow did.
        state = airflow.run_state(bundle["dag_id"], dag_run)
        store.finish(
            dag_run,
            task,
            "rerun",
            done[-1]["attempt"],
            "done" if state == "success" else "failed",
            f"recovered after restart; run state {state}",
        )
        decision = policy.Decision("rerun", "resumed after restart", attempt=done[-1]["attempt"])
    elif inc["status"] in ("acting", "verifying"):
        decision = policy.Decision(
            "rerun", "resumed after restart", attempt=done[-1]["attempt"] if done else 0
        )
    else:
        decision = policy.decide(task, result["triage_class"], result["confidence"], len(done))
        store.update_incident(
            bundle_name,
            decision={"action": decision.action, "reason": decision.reason, "attempt": decision.attempt},
        )
    log(f"  policy: {decision.action} — {decision.reason}")

    if decision.action == "propose_pr":
        patch = write_report(bundle_name, "proposals", result["proposed_patch"])
        write_report(
            bundle_name,
            "escalations",
            f"# {result['pr_title']}\n\n{result['pr_body']}\n\n"
            f"Patch: `{patch}`. Open it as a draft PR; slim CI runs on it.\n",
        )
        store.update_incident(bundle_name, status="escalated", closed_at=None)
        return "escalated"
    if decision.action == "escalate":
        write_report(
            bundle_name,
            "escalations",
            f"# {task} in {dag_run}\n\n{decision.reason}\n\n"
            f"{result['summary']}\n\n" + "\n".join(f"- {e}" for e in result["evidence"]) + "\n",
        )
        store.update_incident(bundle_name, status="escalated")
        return "escalated"

    # 3. act — claim first; an existing receipt means it was already fired
    if inc["status"] not in ("acting", "verifying"):
        try:
            store.claim(dag_run, task, "rerun", decision.attempt, bundle_name)
        except AlreadyClaimed as exc:
            store.update_incident(
                bundle_name,
                status="escalated",
                decision={"action": "escalate", "reason": f"already claimed: {exc}"},
            )
            return "escalated"
        store.update_incident(bundle_name, status="acting")
        try:
            airflow.clear_task(bundle["dag_id"], dag_run, task)
        except Exception as exc:
            store.finish(dag_run, task, "rerun", decision.attempt, "failed", f"clear refused: {exc!r}")
            store.update_incident(bundle_name, status="escalated")
            return "escalated"
        log(f"  re-run #{decision.attempt} of {task} asked; waiting for the run to finish")
    if inc["status"] != "verifying":
        state = airflow.wait_finished(bundle["dag_id"], dag_run)
        if store.receipts(dag_run, task, "rerun")[-1]["status"] == "started":
            store.finish(
                dag_run,
                task,
                "rerun",
                decision.attempt,
                "done" if state == "success" else "failed",
                f"run state {state}",
            )
        if state != "success":
            # The task's next failure writes a new evidence file -> a new incident, bounded by receipts.
            store.update_incident(bundle_name, status="superseded", verification={"dag_run_state": state})
            return "superseded"
        store.update_incident(bundle_name, status="verifying")

    # 4. verify, 5. close
    checks = verify(airflow, bundle)
    if checks["passed"]:
        store.update_incident(bundle_name, status="closed", verification=checks, closed_at=now())
        return "closed"
    store.update_incident(bundle_name, status="escalated", verification=checks)
    write_report(
        bundle_name,
        "escalations",
        f"# {task} in {dag_run}: re-run done, verification FAILED\n\n"
        + "```json\n"
        + json.dumps(checks, indent=2)
        + "\n```\n",
    )
    return "escalated"


def run_once(store: Store, client, airflow, log=print) -> dict[str, str]:
    """Take in every new evidence file, then work every unfinished incident. Returns bundle -> status."""
    inbox = HOME / "inbox"
    for path in sorted(inbox.glob("*.json")) if inbox.exists() else []:
        if store.open_incident(path.name, json.loads(path.read_text(encoding="utf-8"))):
            log(f"new incident: {path.name}")
    outcome = {}
    for inc in store.unfinished():
        bundle = json.loads((inbox / inc["bundle"]).read_text(encoding="utf-8"))
        log(f"{inc['bundle']} ({inc['status']})")
        outcome[inc["bundle"]] = handle(store, client, airflow, inc["bundle"], bundle, log)
        log(f"  -> {outcome[inc['bundle']]}")
    return outcome
