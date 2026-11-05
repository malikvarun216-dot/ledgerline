"""scripts/publish_gold.py: the order of write-audit-publish, checked without reaching Snowflake.

The guarantee is in the order: nothing may write the published schema before the audit build has
passed, and the audit must start from what is published (Session 25).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import publish_gold


def names(plan):
    return [name for name, _ in plan]


def test_audit_build_comes_before_any_publish_step():
    plan = publish_gold.steps("snowflake")
    audit = names(plan).index("write + audit: build in the audit schema, every test")
    for i, (name, command) in enumerate(plan):
        writes_published = "--vars" not in command and command[0] in {"clone", "run", "build"}
        if writes_published:
            assert i > audit, f"{name} writes the published schema before the audit"


def test_every_audit_side_write_carries_the_wap_var():
    for name, command in publish_gold.steps("snowflake", audit_only=True):
        if command[0] in {"clone", "build"}:
            assert command[command.index("--vars") + 1] == "{wap: true}", name


def test_seed_clones_from_the_published_manifest_not_the_audit():
    seed = dict(publish_gold.steps("snowflake"))["seed: incremental facts, published -> audit"]
    assert seed[seed.index("--state") + 1].endswith("published")
    assert "--full-refresh" in seed  # an existing audit table (maybe a failed night) is replaced


def test_publish_clones_from_the_audit_manifest_and_replaces():
    publish = dict(publish_gold.steps("snowflake"))["publish: tables, audit -> published"]
    assert publish[publish.index("--state") + 1].endswith("audit")
    assert "--full-refresh" in publish  # without it dbt clone skips tables that already exist


def test_audit_only_never_publishes():
    plan = names(publish_gold.steps("snowflake", audit_only=True))
    assert not any(n.startswith(("publish:", "check the published")) for n in plan)


def test_ci_needs_a_schema():
    with pytest.raises(SystemExit):
        publish_gold.main(["--target", "ci"])
