"""The failure hook: one evidence file per failed task try, and nothing else (Session 19c).

decisions.md, Session 19c "The triage agent's shape". Airflow calls ``record_failure`` when a task has
failed for good (after its own retries; a retry calls on_retry_callback, not this). The hook writes

    $TRIAGE_INBOX/<dag_id>__<run_id>__<task_id>__try<n>.json

with exclusive create: the same failed try written twice is refused by the filesystem, which is the
dedup. The triage agent (scripts/triage_agent.py) reads the inbox; this hook never calls it.

Two rules, both because the hook runs inside the failing task's own process:
* standard library only — the DAG folder is parsed by Airflow, not by the pipeline's venv;
* it never raises: a broken hook must not replace the task's real error with its own.
"""

from __future__ import annotations

import json
import os
import re
import socket
from datetime import UTC, datetime
from pathlib import Path

LOG_TAIL_LINES = 80
_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


def safe_name(*parts: str) -> str:
    """A file name from run ids that contain ':' and '+' (manual__2026-10-22T10:00:00+00:00)."""
    return "__".join(_UNSAFE.sub("-", str(p)) for p in parts)


def git_sha(repo: Path) -> str | None:
    """HEAD's commit without a git binary: HEAD -> ref file -> or packed-refs."""
    try:
        head = (repo / ".git" / "HEAD").read_text(encoding="utf-8").strip()
        if not head.startswith("ref: "):
            return head
        ref = head[5:]
        loose = repo / ".git" / ref
        if loose.exists():
            return loose.read_text(encoding="utf-8").strip()
        for line in (repo / ".git" / "packed-refs").read_text(encoding="utf-8").splitlines():
            if line.endswith(" " + ref):
                return line.split(" ", 1)[0]
    except OSError:
        return None
    return None


def log_tail(log_root: Path, dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
    """The last lines of this try's log file (Airflow 3's default layout), or '' if not found."""
    path = (
        log_root
        / f"dag_id={dag_id}"
        / f"run_id={run_id}"
        / f"task_id={task_id}"
        / f"attempt={try_number}.log"
    )
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return ""
    return "\n".join(lines[-LOG_TAIL_LINES:])


def build_bundle(context: dict, repo: Path, log_root: Path) -> dict:
    ti = context["ti"]
    dag_run = context.get("dag_run")
    try_number = int(getattr(ti, "try_number", 0) or 0)
    logical = getattr(dag_run, "logical_date", None) or context.get("logical_date")
    return {
        "dag_id": ti.dag_id,
        "run_id": ti.run_id,
        "task_id": ti.task_id,
        "try_number": try_number,
        "max_tries": getattr(ti, "max_tries", None),
        "logical_date": logical.isoformat() if hasattr(logical, "isoformat") else logical,
        "exception": repr(context.get("exception"))[:2000],
        "log_tail": log_tail(log_root, ti.dag_id, ti.run_id, ti.task_id, try_number),
        "git_sha": git_sha(repo),
        "failed_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "host": socket.gethostname(),
    }


def write_bundle(bundle: dict, inbox: Path) -> Path | None:
    """Exclusive create; None when this try's file already exists (written once, never overwritten)."""
    inbox.mkdir(parents=True, exist_ok=True)
    path = inbox / (
        safe_name(bundle["dag_id"], bundle["run_id"], bundle["task_id"], f"try{bundle['try_number']}")
        + ".json"
    )
    try:
        with open(path, "x", encoding="utf-8") as f:
            json.dump(bundle, f, indent=2, sort_keys=True)
    except FileExistsError:
        return None
    return path


def record_failure(context: dict) -> None:
    try:
        repo = Path(os.environ.get("LEDGERLINE_ROOT", "/opt/ledgerline"))
        log_root = Path(os.environ.get("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs"))
        inbox = Path(os.environ.get("TRIAGE_INBOX", "/opt/triage/inbox"))
        path = write_bundle(build_bundle(context, repo, log_root), inbox)
        print(f"triage evidence: {path or 'already written for this try'}")
    except Exception as exc:
        print(f"triage evidence NOT written: {exc!r}")
