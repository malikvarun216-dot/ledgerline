"""The triage agent (Session 19c) without Airflow, Claude or Snowflake: the guarantees that are code.

* the failure hook writes each failed try once and never raises;
* a receipt is claimed once — a second claim (a restart, a second agent) is refused;
* policy, not the model, decides: a "high confidence re-run" of a data check is still escalated,
  and the third re-run of a task in one night is refused;
* a crash between claiming a re-run and finishing it never fires the re-run again;
* an incident closes only after verification passes.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "airflow" / "dags"))

import failure_evidence as fe  # noqa: E402

from triage import agent, policy  # noqa: E402
from triage.store import AlreadyClaimed, Store  # noqa: E402
from triage.tools import inside_repo, is_single_read  # noqa: E402

RUN = "manual__2026-10-22T10:00:00+00:00"


def bundle(task="silver", try_number=2):
    return {
        "dag_id": "ledgerline_nightly",
        "run_id": RUN,
        "task_id": task,
        "try_number": try_number,
        "max_tries": 1,
        "logical_date": "2016-09-04T00:00:00+00:00",
        "exception": "boom",
        "log_tail": "Traceback ...",
        "git_sha": "da45950",
        "failed_at": "2026-10-22T10:05:00+00:00",
        "host": "x",
    }


# --- the failure hook ----------------------------------------------------------------------------


def test_hook_writes_each_try_once(tmp_path):
    first = fe.write_bundle(bundle(), tmp_path)
    assert first is not None and ":" not in first.name and "+" not in first.name
    assert fe.write_bundle(bundle(), tmp_path) is None  # the same try again: refused, not overwritten
    assert fe.write_bundle(bundle(try_number=3), tmp_path) is not None  # a new try is a new file


def test_hook_never_raises(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("TRIAGE_INBOX", str(tmp_path))
    fe.record_failure({})  # no "ti" at all
    assert "NOT written" in capsys.readouterr().out


def test_hook_reads_the_log_tail_and_sha(tmp_path):
    git = tmp_path / "repo" / ".git"
    (git / "refs" / "heads").mkdir(parents=True)
    (git / "HEAD").write_text("ref: refs/heads/dev\n")
    (git / "packed-refs").write_text("# pack-refs\nabc1234def refs/heads/dev\n")
    run = "scheduled__2016-09-04"  # Windows refuses ':' in a folder name; Linux (Airflow) does not
    logs = tmp_path / "logs" / "dag_id=d" / f"run_id={run}" / "task_id=t"
    logs.mkdir(parents=True)
    (logs / "attempt=2.log").write_text("\n".join(f"line {i}" for i in range(200)))
    ti = SimpleNamespace(dag_id="d", run_id=run, task_id="t", try_number=2, max_tries=1)
    b = fe.build_bundle({"ti": ti, "exception": ValueError("x")}, tmp_path / "repo", tmp_path / "logs")
    assert b["git_sha"] == "abc1234def"
    assert (
        b["log_tail"].splitlines()[0] == "line 120" and len(b["log_tail"].splitlines()) == fe.LOG_TAIL_LINES
    )


# --- receipts ------------------------------------------------------------------------------------


def test_a_receipt_is_claimed_once(tmp_path):
    store = Store(tmp_path / "t.db")
    store.claim(RUN, "silver", "rerun", 1, "b.json")
    with pytest.raises(AlreadyClaimed):
        Store(tmp_path / "t.db").claim(RUN, "silver", "rerun", 1, "b.json")  # a second agent, same file


# --- policy --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "task, cls, conf, done, action",
    [
        ("silver", "runbook:rerun_task", "high", 0, "rerun"),
        ("silver", "runbook:rerun_task", "high", 2, "escalate"),  # the third re-run
        ("silver", "runbook:rerun_task", "medium", 0, "escalate"),
        ("dbt_build", "runbook:rerun_task", "high", 0, "escalate"),  # a failed test is a finding
        ("check_raw", "runbook:rerun_task", "high", 0, "escalate"),
        ("new_task", "runbook:rerun_task", "high", 0, "escalate"),  # not listed -> never re-run
        ("dbt_build", "code_change", "high", 0, "propose_pr"),
        ("silver", "unknown", "low", 0, "escalate"),
        ("silver", "delete_and_reload", "high", 0, "escalate"),  # a class the model invented
    ],
)
def test_policy(task, cls, conf, done, action):
    assert policy.decide(task, cls, conf, done).action == action


def test_every_dag_task_is_listed():
    src = (ROOT / "airflow" / "dags" / "ledgerline_nightly.py").read_text(encoding="utf-8")
    import re

    tasks = set(re.findall(r'task_id="([a-z_]+)"', src))
    assert tasks == set(policy.RERUNNABLE) | set(policy.FINDINGS)


# --- read-only tools -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql, ok",
    [
        ("SELECT 1", True),
        ("with x as (select 1) select * from x", True),
        ("SHOW TABLES", True),
        ("DELETE FROM t", False),
        ("SELECT 1; DROP TABLE t", False),
        ("CREATE TABLE t AS SELECT 1", False),
    ],
)
def test_single_read_guard(sql, ok):
    assert is_single_read(sql) is ok


def test_repo_paths_only():
    assert inside_repo("docs/incidents.md") is not None
    assert inside_repo("../outside.txt") is None
    assert inside_repo(".env") is None


# --- the agent, end to end with fakes ------------------------------------------------------------


def submit(cls, conf="high"):
    block = SimpleNamespace(
        type="tool_use",
        name="submit_triage",
        id="t1",
        input={
            "triage_class": cls,
            "confidence": conf,
            "summary": "s",
            "evidence": ["e"],
            "matching_incident": "none",
            "proposed_patch": "--- a\n+++ b\n" if cls == "code_change" else "",
            "pr_title": "fix: x" if cls == "code_change" else "",
            "pr_body": "",
        },
    )
    usage = SimpleNamespace(
        input_tokens=1000, output_tokens=200, cache_read_input_tokens=0, cache_creation_input_tokens=0
    )
    resp = SimpleNamespace(content=[block], usage=usage, model="claude-opus-5-5", stop_reason="tool_use")
    return SimpleNamespace(beta=SimpleNamespace(messages=SimpleNamespace(create=lambda **_: resp)))


class FakeAirflow:
    def __init__(self, final="success"):
        self.cleared, self.final = [], final

    def clear_task(self, dag_id, run_id, task_id):
        self.cleared.append(task_id)

    def wait_finished(self, dag_id, run_id):
        return self.final

    def run_state(self, dag_id, run_id):
        return self.final


@pytest.fixture
def world(tmp_path, monkeypatch):
    monkeypatch.setattr(agent, "HOME", tmp_path)
    (tmp_path / "inbox").mkdir()
    checks = {"exit": 0}
    monkeypatch.setattr(
        agent, "run_check", lambda *a: {"command": " ".join(a), "exit": checks["exit"], "tail": ""}
    )
    return SimpleNamespace(store=Store(tmp_path / "t.db"), inbox=tmp_path / "inbox", checks=checks)


def drop(world, b):
    path = fe.write_bundle(b, world.inbox)
    return path.name


def test_transient_failure_rerun_and_closed(world):
    name = drop(world, bundle())
    af = FakeAirflow()
    assert agent.run_once(world.store, submit("runbook:rerun_task"), af, log=lambda *_: None) == {
        name: "closed"
    }
    assert af.cleared == ["silver"]
    assert [r["status"] for r in world.store.receipts(RUN, "silver", "rerun")] == ["done"]
    assert world.store.spend_usd() == pytest.approx((1000 * 4 + 200 * 20) / 1e6)


def test_failed_verification_escalates(world):
    name = drop(world, bundle())
    world.checks["exit"] = 1
    assert (
        agent.run_once(world.store, submit("runbook:rerun_task"), FakeAirflow(), log=lambda *_: None)[name]
        == "escalated"
    )


def test_a_failed_dbt_test_is_never_rerun(world):
    drop(world, bundle(task="dbt_build"))
    af = FakeAirflow()
    agent.run_once(world.store, submit("runbook:rerun_task"), af, log=lambda *_: None)
    assert af.cleared == []


def test_rerun_bounded_across_incidents(world):
    af = FakeAirflow(final="failed")
    for try_number in (2, 3, 4):  # the task keeps failing: each failure is a new evidence file
        drop(world, bundle(try_number=try_number))
        agent.run_once(world.store, submit("runbook:rerun_task"), af, log=lambda *_: None)
    assert af.cleared == ["silver", "silver"]  # two re-runs, the third failure escalated


def test_restart_after_claim_never_refires(world):
    name = drop(world, bundle())
    world.store.open_incident(name, bundle())
    world.store.update_incident(
        name,
        triage=json.dumps(submit("runbook:rerun_task").beta.messages.create().content[0].input),
        status="acting",
    )
    world.store.claim(RUN, "silver", "rerun", 1, name)  # the crash happened right after this
    af = FakeAirflow()
    assert agent.run_once(world.store, None, af, log=lambda *_: None)[name] == "closed"
    assert af.cleared == []  # Airflow's state was read; the re-run was not asked again
    assert world.store.receipts(RUN, "silver", "rerun")[0]["status"] == "done"


def test_code_change_becomes_a_proposal(world):
    name = drop(world, bundle(task="dbt_build"))
    agent.run_once(world.store, submit("code_change"), FakeAirflow(), log=lambda *_: None)
    assert (world.inbox.parent / "proposals" / name.replace(".json", ".patch")).exists()
