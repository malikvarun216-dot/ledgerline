"""The triage agent's tools: every one READS, none writes (Session 19c).

decisions.md, Session 19c. Claude gets these and nothing else. The guarantee is where each tool
runs, not what the prompt says:

* ``snowflake_select`` signs in as LEDGERLINE_CI_SVC with role LEDGERLINE_CI_RUNNER, which can SELECT
  RAW / STAGING / SNAPSHOTS / GOLD and cannot write LEDGERLINE (S16). The one-statement guard below is
  a second wall that stops wasted calls; the role is the wall that holds.
* ``git_*`` run fixed read-only git subcommands, never a command line the model wrote.
* ``read_repo_file`` / ``search_repo`` refuse any path outside the repository.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MAX_CHARS = 12_000  # per tool result: enough evidence, bounded spend
MAX_ROWS = 50

_ONE_READ = re.compile(r"^\s*(SELECT|WITH|SHOW|DESCRIBE|DESC)\b", re.IGNORECASE)
_SHA = re.compile(r"^[0-9a-f]{7,40}$")


def clip(text: str) -> str:
    return text if len(text) <= MAX_CHARS else text[:MAX_CHARS] + f"\n… [clipped at {MAX_CHARS} chars]"


def is_single_read(sql: str) -> bool:
    """One statement that starts as a read. `;` inside is refused outright (no second statement)."""
    body = sql.strip().rstrip(";")
    return bool(_ONE_READ.match(body)) and ";" not in body


def inside_repo(path: str) -> Path | None:
    p = (ROOT / path).resolve()
    return p if p.is_relative_to(ROOT) and ".git" not in p.parts and ".env" != p.name else None


def git(*args: str) -> str:
    out = subprocess.run(
        ["git", "-C", str(ROOT), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    return clip(out.stdout + (f"\n[stderr] {out.stderr}" if out.returncode else ""))


# --- the tools ------------------------------------------------------------------------------------


def git_log(n: int = 15) -> str:
    return git("log", f"-{max(1, min(int(n), 50))}", "--date=short", "--format=%h %ad %s", "--stat")


def git_show(sha: str) -> str:
    if not _SHA.match(sha or ""):
        return "refused: give a commit sha (7-40 hex characters)"
    return git("show", "--stat", "--patch", sha)


def git_diff_since(sha: str) -> str:
    """What changed in the repository since the commit the failed task ran with."""
    if not _SHA.match(sha or ""):
        return "refused: give a commit sha (7-40 hex characters)"
    return git("diff", "--stat", "--patch", f"{sha}..HEAD")


def read_repo_file(path: str, start: int = 1, lines: int = 200) -> str:
    p = inside_repo(path)
    if p is None or not p.is_file():
        return f"refused or not found: {path}"
    text = p.read_text(encoding="utf-8", errors="replace").splitlines()
    s = max(1, int(start))
    chunk = text[s - 1 : s - 1 + max(1, min(int(lines), 400))]
    return clip("\n".join(f"{s + i}: {line}" for i, line in enumerate(chunk)))


def search_repo(pattern: str, path: str = ".") -> str:
    """Fixed-string search, e.g. an error text in docs/incidents.md (known failure modes)."""
    p = inside_repo(path)
    if p is None:
        return f"refused: {path}"
    return git(
        "grep", "-n", "-I", "-F", "--max-count=20", "-e", pattern, "--", str(p.relative_to(ROOT)) or "."
    )


def snowflake_select(sql: str) -> str:
    if not is_single_read(sql):
        return "refused: one SELECT / WITH / SHOW / DESCRIBE statement only"
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    from sf import connect

    conn = connect("LEDGERLINE_CI_RUNNER", as_ci=True)
    try:
        cur = conn.cursor()
        cur.execute("USE WAREHOUSE LEDGERLINE_WH")
        cur.execute(sql, timeout=120)
        cols = [d[0] for d in cur.description or []]
        rows = cur.fetchmany(MAX_ROWS)
    except Exception as exc:
        return f"query failed: {exc}"
    finally:
        conn.close()
    return clip("\n".join([" | ".join(cols), *(" | ".join(map(str, r)) for r in rows)]))


def raw_field_diff(table_name: str) -> str:
    """Top-level fields of RAW.SILVER_CHANGES.RECORD in the newest export folder vs all older ones:
    the "what changed in the data's shape" part of the evidence."""
    if not re.fullmatch(r"[a-z_]+", table_name or ""):
        return "refused: table_name is a Silver table name, e.g. orders"
    out = snowflake_select(f"""
        SELECT SPLIT_PART(SOURCE_FILE, '/', 2) AS folder, ARRAY_AGG(DISTINCT f.key) AS fields
        FROM LEDGERLINE.RAW.SILVER_CHANGES, LATERAL FLATTEN(RECORD) f
        WHERE TABLE_NAME = '{table_name}'
        GROUP BY 1 ORDER BY 1""")
    return out


TOOLS = {
    "git_log": (
        git_log,
        "Recent commits with changed files. Use to see what changed before the failure.",
        {"n": {"type": "integer", "description": "how many commits, 1-50"}},
        [],
    ),
    "git_show": (git_show, "One commit's message and patch.", {"sha": {"type": "string"}}, ["sha"]),
    "git_diff_since": (
        git_diff_since,
        "Changes in the repository since the commit the failed task ran with "
        "(the bundle's git_sha) up to HEAD.",
        {"sha": {"type": "string"}},
        ["sha"],
    ),
    "read_repo_file": (
        read_repo_file,
        "Read lines of a file in the repository (code, SQL, docs).",
        {"path": {"type": "string"}, "start": {"type": "integer"}, "lines": {"type": "integer"}},
        ["path"],
    ),
    "search_repo": (
        search_repo,
        "Search the repository for a fixed string. docs/incidents.md holds every "
        "known failure mode with its prevention rule; docs/constraints.md every platform fact.",
        {"pattern": {"type": "string"}, "path": {"type": "string"}},
        ["pattern"],
    ),
    "snowflake_select": (
        snowflake_select,
        "Run ONE read-only query on Snowflake (role LEDGERLINE_CI_RUNNER: "
        "reads LEDGERLINE.RAW / STAGING / SNAPSHOTS / GOLD; GOLD text columns may be masked). "
        "Fully qualify names. At most 50 rows come back.",
        {"sql": {"type": "string"}},
        ["sql"],
    ),
    "raw_field_diff": (
        raw_field_diff,
        "The fields each export folder of one Silver table delivered to "
        "RAW, oldest first: shows a field added or dropped upstream.",
        {"table_name": {"type": "string"}},
        ["table_name"],
    ),
}


def tool_specs() -> list[dict]:
    specs = []
    for name, (_, desc, props, required) in TOOLS.items():
        specs.append(
            {
                "name": name,
                "description": desc,
                "input_schema": {
                    "type": "object",
                    "properties": props,
                    "required": required,
                    "additionalProperties": False,
                },
            }
        )
    return specs


def run_tool(name: str, args: dict) -> tuple[str, bool]:
    """(result text, is_error). Unknown tool or bad arguments come back as an error result."""
    if name not in TOOLS:
        return f"no tool named {name}", True
    try:
        return TOOLS[name][0](**args), False
    except Exception as exc:
        return f"{name} failed: {exc!r}", True
