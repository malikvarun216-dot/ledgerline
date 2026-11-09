"""The working tree holds LF, like the repository — not only the repository.

Session 16: `.gitattributes` pins `eol=lf`, yet 19 tracked files sat in the working tree with CRLF
(saved by a Windows editor after checkout). Git normalises on commit, so the repository stayed LF and
`git status` showed nothing — invisible, as in the 2026-09-19 blob-sink incident. dbt is not: it
checksums the bytes on disk, so slim CI on the laptop compared CRLF snapshots and a CRLF test macro
against the LF base branch and selected 18 nodes for a change that did not exist.

Session 31: Power BI Desktop rewrites every PBIP file it owns with CRLF on each save, so 59 files under
`powerbi/` failed this test from S23 on. Those files are pinned `eol=crlf` in `.gitattributes` (the
repository still stores LF); a CRLF file is accepted only where its attribute asks for CRLF, and that
attribute is allowed only under `powerbi/` — never where dbt or Spark reads bytes.

On a Linux CI runner this always passes (a fresh checkout is LF). It is the laptop it protects.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CRLF_ALLOWED_UNDER = "powerbi/"


def _eol_lines() -> list[str]:
    lines = subprocess.run(
        ["git", "ls-files", "--eol"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.splitlines()
    assert len(lines) > 100, "git ls-files returned almost nothing — wrong directory?"
    return lines


def test_no_tracked_file_is_crlf_unless_pinned_crlf():
    wrong = [
        line.split("\t")[-1]
        for line in _eol_lines()
        if "w/mixed" in line or ("w/crlf" in line and "eol=crlf" not in line)
    ]
    assert not wrong, (
        f"CRLF in the working tree (the repository is LF): {wrong}. "
        "Rewrite them with LF; git will show no content change."
    )


def test_crlf_pin_stays_inside_powerbi():
    pinned = [line.split("\t")[-1] for line in _eol_lines() if "eol=crlf" in line]
    outside = [path for path in pinned if not path.startswith(CRLF_ALLOWED_UNDER)]
    assert not outside, (
        f"eol=crlf reaches files outside {CRLF_ALLOWED_UNDER}: {outside}. "
        "Only Power BI Desktop's own files may be CRLF on disk."
    )
