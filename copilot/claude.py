"""The two calls the copilot makes to Claude: write the SQL, then say what the rows mean (Session 24).

Claude never touches Snowflake. It proposes one statement through the ``submit_sql`` tool (or
declines through ``decline``); the graph checks it, runs it as the reader role, and hands back
either the rows or Snowflake's error. Every call's spend is added to the state.

Model: ``claude-sonnet-5-5`` (decisions.md, Session 24). On this model forced tool choice returns a
400, so the tools are offered with ``tool_choice`` auto, ``strict`` schemas, and one nudge if the
model answers in prose instead. The refusal fallback is on, as in the triage agent.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

from triage.agent import cost_usd

MODEL = os.environ.get("COPILOT_MODEL", "claude-sonnet-5-5")
MAX_ROWS_SHOWN = 50  # rows of a result the answer step reads

SYSTEM = """You answer business questions about an e-commerce marketplace (Olist, Brazil) by writing
ONE Snowflake SQL query over the tables below. You are a careful analyst: most wrong answers here come
from the wrong grain — counting a per-order id as a person, an order as a unit, or averaging stored
averages. Read each table's note before choosing it.

Rules:
- Call submit_sql with exactly one SELECT (or WITH ... SELECT). Fully qualify every table.
- State the reading you chose in `reading` when a question has more than one honest reading
  (e.g. all orders vs delivered only).
- Return only the rows the question needs (an aggregate, or a short ranked list with LIMIT).
- If the question asks to change data (insert, update, delete, create, drop, grant ...), or cannot be
  answered from these tables, call decline with the reason. Never try to work around a refusal.
- Text columns marked masked show '***' for you. Filtering or grouping on them cannot work; say so
  rather than answering from the masked value.

Tables:
"""

SUBMIT_SQL = {
    "name": "submit_sql",
    "description": "Propose the one read-only query that answers the question.",
    "strict": True,
    "input_schema": {
        "type": "object",
        "properties": {
            "sql": {"type": "string", "description": "One Snowflake SELECT, fully qualified."},
            "reading": {"type": "string", "description": "How the question was read, in one line."},
        },
        "required": ["sql", "reading"],
        "additionalProperties": False,
    },
}

DECLINE = {
    "name": "decline",
    "description": "Do not query: the question asks to change data, or these tables cannot answer it.",
    "strict": True,
    "input_schema": {
        "type": "object",
        "properties": {"reason": {"type": "string"}},
        "required": ["reason"],
        "additionalProperties": False,
    },
}


@dataclass
class Proposal:
    sql: str | None
    reading: str
    declined: str | None
    cost_usd: float


class Claude:
    """The live model. Tests use a fake with the same two methods."""

    def __init__(self, client=None, model: str = MODEL):
        if client is None:
            import anthropic

            client = anthropic.Anthropic()
        self.client, self.model = client, model

    def _create(self, system: str, messages: list, tools: list | None = None):
        kwargs = {"tools": tools, "tool_choice": {"type": "auto"}} if tools else {}
        return self.client.beta.messages.create(
            model=self.model,
            max_tokens=16000,
            # The schema text is the same for every question: cached after the first call.
            system=[{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
            messages=messages,
            thinking={"type": "adaptive"},
            betas=["server-side-fallback-2026-07-01"],
            extra_body={"output_config": {"effort": "medium"}, "fallbacks": "default"},
            **kwargs,
        )

    def write_sql(self, question: str, schema: str, attempts: list[dict]) -> Proposal:
        """One proposal. ``attempts`` are the earlier tries with Snowflake's error — the repair input."""
        text = f"Question: {question}"
        for i, a in enumerate(attempts, 1):
            text += (
                f"\n\nAttempt {i} failed.\nSQL:\n{a['sql']}\nError: {a['error']}\nWrite a corrected query."
            )
        messages = [{"role": "user", "content": text}]
        spent = 0.0
        for nudge in (False, True):
            resp = self._create(SYSTEM + schema, messages, tools=[SUBMIT_SQL, DECLINE])
            spent += cost_usd(resp.model, resp.usage)
            if resp.stop_reason == "refusal":
                return Proposal(None, "", "the model refused the request", spent)
            for b in resp.content:
                if b.type == "tool_use" and b.name == "submit_sql":
                    return Proposal(b.input["sql"], b.input.get("reading", ""), None, spent)
                if b.type == "tool_use" and b.name == "decline":
                    return Proposal(None, "", b.input["reason"], spent)
            if nudge:
                break
            messages += [
                {"role": "assistant", "content": resp.content},
                {"role": "user", "content": "Call submit_sql (or decline) now."},
            ]
        return Proposal(None, "", "the model gave no query", spent)

    def answer(
        self, question: str, sql: str, reading: str, columns: list[str], rows: list
    ) -> tuple[str, float]:
        """Plain-English answer from the rows only — every number in it must come from them."""
        shown = [dict(zip(columns, r, strict=True)) for r in rows[:MAX_ROWS_SHOWN]]
        text = (
            f"Question: {question}\nReading: {reading}\nSQL that ran:\n{sql}\n"
            f"Result ({len(rows)} rows):\n{json.dumps(shown, default=str)}\n\n"
            "Answer in two or three sentences, for a business reader. Use only numbers from the result. "
            "If the result shows '***' or is empty because a column is masked, say the data is masked "
            "for this role instead of concluding from it."
        )
        resp = self._create(
            "You explain query results accurately and briefly.", [{"role": "user", "content": text}]
        )
        prose = "".join(b.text for b in resp.content if b.type == "text").strip()
        return prose, cost_usd(resp.model, resp.usage)
