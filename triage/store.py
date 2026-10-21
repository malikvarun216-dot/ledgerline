"""The triage agent's memory of what it did: incidents, action receipts, AI calls (Session 19c).

decisions.md, Session 19c "The triage agent's shape". SQLite, because its PRIMARY KEY is enforced:
claiming a repair is an INSERT, and a second claim of the same repair fails instead of running twice.
(Snowflake would accept both rows — its keys are labels, not constraints.)

    incidents  one per evidence file: what failed, Claude's class, the policy's decision, the close
    receipts   (dag_run_id, task_id, action, attempt): written BEFORE the action, closed after it
    ai_calls   every Claude call: model, tokens, dollars — the spend is read, not assumed
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS incidents (
    bundle        TEXT PRIMARY KEY,            -- the evidence file's name: one incident per failed try
    dag_id        TEXT NOT NULL,
    run_id        TEXT NOT NULL,
    task_id       TEXT NOT NULL,
    try_number    INTEGER NOT NULL,
    status        TEXT NOT NULL,               -- open / acting / verifying / closed / escalated / superseded
    triage_class  TEXT,                        -- runbook:rerun_task / code_change / unknown
    triage        TEXT,                        -- Claude's whole submit_triage input, JSON
    decision      TEXT,                        -- what policy.py allowed, and why
    verification  TEXT,                        -- the checks run before closing, JSON
    opened_at     TEXT NOT NULL,
    closed_at     TEXT
);
CREATE TABLE IF NOT EXISTS receipts (
    dag_run_id  TEXT NOT NULL,
    task_id     TEXT NOT NULL,
    action      TEXT NOT NULL,
    attempt     INTEGER NOT NULL,
    bundle      TEXT NOT NULL,
    status      TEXT NOT NULL,                 -- started / done / failed
    result      TEXT,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    PRIMARY KEY (dag_run_id, task_id, action, attempt)
);
CREATE TABLE IF NOT EXISTS ai_calls (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    bundle        TEXT NOT NULL,
    model         TEXT NOT NULL,
    turn          INTEGER NOT NULL,
    input_tokens  INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    cache_read    INTEGER NOT NULL,
    cost_usd      REAL NOT NULL,
    stop_reason   TEXT,
    request_id    TEXT,
    called_at     TEXT NOT NULL
);
"""


def now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


class AlreadyClaimed(Exception):
    """This exact action was already started once — by this agent before a restart, or another copy."""


class Store:
    def __init__(self, path: Path | str):
        self.db = sqlite3.connect(str(path), isolation_level=None)  # autocommit: each write lands at once
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)

    # incidents -------------------------------------------------------------------------------------
    def open_incident(self, bundle_name: str, bundle: dict) -> bool:
        """True when this evidence file is new; False when it was already taken in."""
        cur = self.db.execute(
            "INSERT OR IGNORE INTO incidents (bundle, dag_id, run_id, task_id, try_number, status, opened_at)"
            " VALUES (?, ?, ?, ?, ?, 'open', ?)",
            (bundle_name, bundle["dag_id"], bundle["run_id"], bundle["task_id"], bundle["try_number"], now()),
        )
        return cur.rowcount == 1

    def incident(self, bundle_name: str) -> sqlite3.Row | None:
        return self.db.execute("SELECT * FROM incidents WHERE bundle = ?", (bundle_name,)).fetchone()

    def update_incident(self, bundle_name: str, **fields) -> None:
        for key in ("triage", "decision", "verification"):
            if key in fields and not isinstance(fields[key], str):
                fields[key] = json.dumps(fields[key], sort_keys=True)
        cols = ", ".join(f"{k} = ?" for k in fields)
        self.db.execute(f"UPDATE incidents SET {cols} WHERE bundle = ?", (*fields.values(), bundle_name))

    def unfinished(self) -> list[sqlite3.Row]:
        return self.db.execute(
            "SELECT * FROM incidents WHERE status NOT IN ('closed', 'escalated', 'superseded')"
            " ORDER BY opened_at"
        ).fetchall()

    # receipts --------------------------------------------------------------------------------------
    def claim(self, dag_run_id: str, task_id: str, action: str, attempt: int, bundle_name: str) -> None:
        """Write the receipt BEFORE acting. Raises AlreadyClaimed if it exists in any state."""
        try:
            self.db.execute(
                "INSERT INTO receipts (dag_run_id, task_id, action, attempt, bundle, status, started_at)"
                " VALUES (?, ?, ?, ?, ?, 'started', ?)",
                (dag_run_id, task_id, action, attempt, bundle_name, now()),
            )
        except sqlite3.IntegrityError as exc:
            raise AlreadyClaimed(f"{action} #{attempt} on {task_id} in {dag_run_id}") from exc

    def finish(
        self, dag_run_id: str, task_id: str, action: str, attempt: int, status: str, result: str
    ) -> None:
        self.db.execute(
            "UPDATE receipts SET status = ?, result = ?, finished_at = ?"
            " WHERE dag_run_id = ? AND task_id = ? AND action = ? AND attempt = ?",
            (status, result, now(), dag_run_id, task_id, action, attempt),
        )

    def receipts(self, dag_run_id: str, task_id: str, action: str) -> list[sqlite3.Row]:
        return self.db.execute(
            "SELECT * FROM receipts WHERE dag_run_id = ? AND task_id = ? AND action = ? ORDER BY attempt",
            (dag_run_id, task_id, action),
        ).fetchall()

    # spend -----------------------------------------------------------------------------------------
    def log_call(
        self,
        bundle_name: str,
        model: str,
        turn: int,
        usage,
        cost_usd: float,
        stop_reason: str | None,
        request_id: str | None,
    ) -> None:
        self.db.execute(
            "INSERT INTO ai_calls (bundle, model, turn, input_tokens, output_tokens, cache_read, cost_usd,"
            " stop_reason, request_id, called_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                bundle_name,
                model,
                turn,
                usage.input_tokens,
                usage.output_tokens,
                getattr(usage, "cache_read_input_tokens", 0) or 0,
                cost_usd,
                stop_reason,
                request_id,
                now(),
            ),
        )

    def spend_usd(self, since: str = "") -> float:
        row = self.db.execute(
            "SELECT COALESCE(SUM(cost_usd), 0) FROM ai_calls WHERE called_at >= ?", (since,)
        ).fetchone()
        return float(row[0])
