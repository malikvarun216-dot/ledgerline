"""Ask Cortex Analyst the same question, as the same user and role (Session 24).

Cortex Analyst's REST API (``POST /api/v2/cortex/analyst/message``) takes the question and a
semantic view and returns its interpretation plus one SQL statement; it does not run the SQL. The
call is signed with the session token of the copilot's own Snowflake connection, so Cortex works
under the reader role, and its SQL then goes through the copilot's ``check`` and ``run`` — the same
guard, role and grader. decisions.md, Session 24.
"""

from __future__ import annotations

import os
import time

SEMANTIC_VIEW = "LEDGERLINE.S24.GOLD_SV"


def ask(runner, question: str, semantic_view: str = SEMANTIC_VIEW, timeout: float = 120) -> dict:
    """Return {"sql", "text", "warnings", "seconds", "request_id"}; ``sql`` is None if Cortex gave none."""
    import requests

    host = f"https://{os.environ['SNOWFLAKE_ACCOUNT'].lower()}.snowflakecomputing.com"
    started = time.monotonic()
    resp = requests.post(
        f"{host}/api/v2/cortex/analyst/message",
        headers={
            "Authorization": f'Snowflake Token="{runner.conn.rest.token}"',
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json={
            "messages": [{"role": "user", "content": [{"type": "text", "text": question}]}],
            "semantic_view": semantic_view,
            "stream": False,
        },
        timeout=timeout,
    )
    seconds = round(time.monotonic() - started, 1)
    if resp.status_code != 200:
        return {
            "sql": None,
            "text": f"HTTP {resp.status_code}: {resp.text[:500]}",
            "warnings": [],
            "seconds": seconds,
            "request_id": resp.headers.get("x-snowflake-request-id"),
        }
    body = resp.json()
    content = body.get("message", {}).get("content", [])
    sql = next((c["statement"] for c in content if c.get("type") == "sql"), None)
    text = " ".join(c.get("text", "") for c in content if c.get("type") == "text").strip()
    return {
        "sql": sql,
        "text": text,
        "warnings": [w.get("message", w) for w in body.get("warnings", [])],
        "seconds": seconds,
        "request_id": body.get("request_id"),
    }
