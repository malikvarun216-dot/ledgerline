"""Databricks notebooks in git must be byte-identical to what Databricks saves.

Drill 1: a Git-folder Pull stopped on a merge conflict in a file nobody had edited. Databricks
re-saves a notebook whenever it is opened or run, and its save does two things our files did
not: it adds a serverless-environment header after the first line, and it drops the final
newline. Six notebooks showed as "modified", and the one whose last lines had also changed
upstream conflicted.

The fix is to commit notebooks in exactly that form (decisions.md, "Notebooks pin the serverless
environment version"). These tests keep it true for every notebook added later — Silver's
included — and keep the pinned version the same everywhere, so a change of version is one
deliberate commit, not a drift.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MARKER = b"# Databricks notebook source"
HEADER = [
    "# /// script",
    "# [tool.databricks.environment]",
    '# environment_version = "6"',
    "# ///",
]


def _notebooks() -> list[Path]:
    tracked = subprocess.run(
        ["git", "ls-files", "*.py"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.split()
    return [ROOT / f for f in tracked if (ROOT / f).read_bytes().startswith(MARKER)]


def test_there_are_notebooks_to_check():
    # Without this, an empty list (git missing, wrong cwd) would make both tests below pass.
    assert len(_notebooks()) >= 11


def test_every_notebook_pins_the_same_serverless_environment():
    wrong = []
    for path in _notebooks():
        lines = path.read_bytes().decode("utf-8").replace("\r\n", "\n").split("\n")
        if lines[1:5] != HEADER:
            wrong.append(path.relative_to(ROOT).as_posix())
    assert not wrong, f"missing or different environment header: {wrong}"


def test_no_notebook_ends_with_a_newline():
    """Databricks saves without one; a notebook with one shows as modified after every run."""
    wrong = [p.relative_to(ROOT).as_posix() for p in _notebooks() if p.read_bytes().endswith(b"\n")]
    assert not wrong, f"final newline present (Databricks will strip it): {wrong}"
