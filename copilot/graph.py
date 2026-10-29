"""The copilot as a LangGraph graph: write -> check -> run -> answer, with a capped repair loop (Session 24).

    START -> write_sql --declined--------------------------------> refuse -> END
                 |
                 v
               check --not a single read of Gold---------------> refuse -> END
                 |  \\--does not parse--+
                 v                      |
              run_sql --Snowflake error-+--> (repairs left?) write_sql again, with the error
                 |                                   \\--none left--> give_up -> END
                 v
              answer -> END

Why a graph (decisions.md, 2026-10-24): the repair path is a real branch with a cap, and each node
can be tested alone. Two branches are deliberately NOT repairs:
- a refusal by ``check`` ends the run. A model told "DELETE is not allowed" and asked to try again
  is being asked to find a way around the rule.
- a decline by the model ends the run too. Declining is a correct answer to a write request.

The ROLE is the guarantee (masks, row policy, no writes); ``check`` only makes a refusal visible.
"""

from __future__ import annotations

from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from copilot import sql_guard

MAX_REPAIRS = 2  # at most 3 statements per question


class SqlError(RuntimeError):
    """Snowflake rejected the statement. ``str(err)`` is Snowflake's message, given to the repair."""


class State(TypedDict, total=False):
    question: str
    schema: str
    sql: str | None
    reading: str
    attempts: list[dict]  # every failed try: {"sql", "error"}
    failed: bool  # the last check / run rejected the current statement
    columns: list[str]
    rows: list[Any]
    query_id: str | None
    answer: str
    outcome: str  # answered | refused | gave_up
    refused_by: str  # model | guard
    cost_usd: float
    path: list[str]  # nodes visited, in order — the evidence of what the graph did


def build(llm, runner):
    """``llm`` has ``write_sql`` and ``answer`` (copilot.claude.Claude, or a fake); ``runner`` has
    ``run(sql) -> (columns, rows, query_id)`` raising ``SqlError``."""

    def _step(state: State, name: str, **update) -> dict:
        return {"path": [*state.get("path", []), name], **update}

    def write_sql(state: State) -> dict:
        p = llm.write_sql(state["question"], state["schema"], state.get("attempts", []))
        cost = state.get("cost_usd", 0.0) + p.cost_usd
        if p.declined:
            return _step(state, "write_sql", sql=None, answer=f"Declined: {p.declined}", cost_usd=cost)
        return _step(state, "write_sql", sql=p.sql, reading=p.reading, cost_usd=cost)

    def check(state: State) -> dict:
        try:
            sql_guard.check(state["sql"])
        except sql_guard.Refused as err:
            return _step(state, "check", answer=f"Refused: {err}. This copilot only reads Gold.")
        except SyntaxError as err:
            return _step(
                state,
                "check",
                failed=True,
                attempts=[*state.get("attempts", []), {"sql": state["sql"], "error": str(err)}],
            )
        return _step(state, "check", failed=False)

    def run_sql(state: State) -> dict:
        try:
            columns, rows, query_id = runner.run(state["sql"])
        except SqlError as err:
            return _step(
                state,
                "run_sql",
                failed=True,
                attempts=[*state.get("attempts", []), {"sql": state["sql"], "error": str(err)}],
            )
        return _step(state, "run_sql", failed=False, columns=columns, rows=rows, query_id=query_id)

    def answer(state: State) -> dict:
        prose, cost = llm.answer(
            state["question"], state["sql"], state.get("reading", ""), state["columns"], state["rows"]
        )
        return _step(
            state, "answer", answer=prose, outcome="answered", cost_usd=state.get("cost_usd", 0.0) + cost
        )

    def refuse(state: State) -> dict:
        # No SQL means the model declined; SQL here means it wrote one and the check stopped it.
        return _step(
            state, "refuse", outcome="refused", refused_by="model" if state.get("sql") is None else "guard"
        )

    def give_up(state: State) -> dict:
        last = state["attempts"][-1]["error"]
        return _step(
            state,
            "give_up",
            outcome="gave_up",
            answer=f"No answer after {len(state['attempts'])} failed queries. Last error: {last}",
        )

    def _repair_or_give_up(state: State) -> str:
        return "write_sql" if len(state["attempts"]) <= MAX_REPAIRS else "give_up"

    def after_write(state: State) -> str:
        return "refuse" if state.get("sql") is None else "check"

    def after_check(state: State) -> str:
        if state.get("answer", "").startswith("Refused:"):
            return "refuse"
        return _repair_or_give_up(state) if state["failed"] else "run_sql"

    def after_run(state: State) -> str:
        return _repair_or_give_up(state) if state["failed"] else "answer"

    g = StateGraph(State)
    for name, fn in [
        ("write_sql", write_sql),
        ("check", check),
        ("run_sql", run_sql),
        ("answer", answer),
        ("refuse", refuse),
        ("give_up", give_up),
    ]:
        g.add_node(name, fn)
    g.add_edge(START, "write_sql")
    g.add_conditional_edges("write_sql", after_write, ["check", "refuse"])
    g.add_conditional_edges("check", after_check, ["run_sql", "refuse", "write_sql", "give_up"])
    g.add_conditional_edges("run_sql", after_run, ["answer", "write_sql", "give_up"])
    for leaf in ("answer", "refuse", "give_up"):
        g.add_edge(leaf, END)
    return g.compile()


def ask(graph, question: str, schema: str) -> State:
    """Run one question to the end. 3 statements x 3 nodes + 2 is far under LangGraph's step limit."""
    return graph.invoke({"question": question, "schema": schema, "attempts": [], "path": [], "cost_usd": 0.0})
