"""The one write the agent has: ask Airflow to clear (= re-run) a failed task (Session 19c).

Airflow 3's REST API on http://localhost:8080 (bound to this machine only), signed in with the local
admin user. Clearing a task with ``include_downstream`` also clears the tasks that were skipped as
``upstream_failed`` behind it, and ``reset_dag_runs`` puts the run back to running — the same thing
the UI's "Clear" button does. The agent never triggers a new run and never marks anything success.
"""

from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request

BASE = os.environ.get("AIRFLOW_API", "http://localhost:8080")
FINISHED = {"success", "failed"}


class Airflow:
    def __init__(self, base: str = BASE, user: str | None = None, password: str | None = None):
        self.base = base.rstrip("/")
        self.user = user or os.environ.get("AIRFLOW_USER", "admin")
        self.password = password or os.environ.get("AIRFLOW_PASSWORD", "admin")
        self._token: str | None = None

    def _call(self, method: str, path: str, body: dict | None = None, auth: bool = True) -> dict:
        headers = {"Content-Type": "application/json"}
        if auth:
            headers["Authorization"] = f"Bearer {self.token()}"
        req = urllib.request.Request(
            self.base + path,
            method=method,
            headers=headers,
            data=json.dumps(body).encode() if body is not None else None,
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read() or b"{}")

    def token(self) -> str:
        if self._token is None:
            self._token = self._call(
                "POST", "/auth/token", {"username": self.user, "password": self.password}, auth=False
            )["access_token"]
        return self._token

    @staticmethod
    def _q(s: str) -> str:
        return urllib.parse.quote(s, safe="")

    def run_state(self, dag_id: str, run_id: str) -> str:
        return self._call("GET", f"/api/v2/dags/{self._q(dag_id)}/dagRuns/{self._q(run_id)}")["state"]

    def task_states(self, dag_id: str, run_id: str) -> dict[str, str]:
        out = self._call("GET", f"/api/v2/dags/{self._q(dag_id)}/dagRuns/{self._q(run_id)}/taskInstances")
        return {ti["task_id"]: ti["state"] for ti in out["task_instances"]}

    def clear_task(self, dag_id: str, run_id: str, task_id: str) -> dict:
        return self._call(
            "POST",
            f"/api/v2/dags/{self._q(dag_id)}/clearTaskInstances",
            {
                "dry_run": False,
                "dag_run_id": run_id,
                "task_ids": [task_id],
                "include_downstream": True,
                "only_failed": False,
                "reset_dag_runs": True,
            },
        )

    def wait_finished(self, dag_id: str, run_id: str, timeout_s: int = 3 * 3600, every_s: int = 30) -> str:
        deadline = time.monotonic() + timeout_s
        while True:
            state = self.run_state(dag_id, run_id)
            if state in FINISHED or time.monotonic() > deadline:
                return state
            time.sleep(every_s)
