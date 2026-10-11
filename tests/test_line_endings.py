"""The working tree holds LF, like the repository — not only the repository.

Session 16: `.gitattributes` pins `eol=lf`, yet 19 tracked files sat in the working tree with CRLF
(saved by a Windows editor after checkout). Git normalises on commit, so the repository stayed LF and
`git status` showed nothing — invisible, as in the 2026-09-19 blob-sink incident. dbt is not: it
checksums the bytes on disk, so slim CI on the laptop compared CRLF snapshots and a CRLF test macro
against the LF base branch and selected 18 nodes for a change that did not exist.

On a Linux CI runner this always passes (a fresh checkout is LF). It is the laptop it protects.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def test_no_tracked_file_is_crlf_in_the_working_tree():
    lines = subprocess.run(
        ["git", "ls-files", "--eol"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.splitlines()
    assert len(lines) > 100, "git ls-files returned almost nothing — wrong directory?"
    crlf = [line.split("\t")[-1] for line in lines if "w/crlf" in line or "w/mixed" in line]
    assert not crlf, (
        f"CRLF in the working tree (the repository is LF): {crlf}. "
        "Rewrite them with LF; git will show no content change."
    )
