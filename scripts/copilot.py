"""The analyst copilot from the command line: compute the key, ask, grade (Session 24).

    python scripts/copilot.py keys                              # the answer key, as the copilot's role
    python scripts/copilot.py ask "How many sellers are at risk?"
    python scripts/copilot.py eval                              # 20 questions, the copilot
    python scripts/copilot.py eval --tool cortex                # the same 20, Cortex Analyst
    python scripts/copilot.py eval --role LEDGERLINE_ANALYST_SP --only Q3,Q18

Signs in to Snowflake as LEDGERLINE_COPILOT_SVC (``.env``: SNOWFLAKE_COPILOT_USER /
SNOWFLAKE_COPILOT_PRIVATE_KEY_PATH) under the reader role given; the Claude key comes from
``.env.copilot`` (ANTHROPIC_API_KEY), never ``.env``. ``eval`` writes one JSON line per question to
``data/copilot/`` — the SQL that ran, its query id, the rows, the grade, the cost — and prints the
scorecard. ``keys`` and ``--tool cortex`` spend no Claude tokens.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from copilot import cortex, grade, graph, sql_guard  # noqa: E402
from copilot.facts import prompt_block  # noqa: E402
from generators._common import load_local_env  # noqa: E402

OUT = ROOT / "data" / "copilot"


def keys(runner, questions: list[dict]) -> dict[str, dict]:
    out = {}
    for q in questions:
        if "key" not in q:
            continue
        columns, rows, _ = runner.run(q["key"])
        out[q["id"]] = dict(zip([c.lower() for c in columns], rows[0], strict=True))
    return out


def ask_cortex(runner, question: str) -> dict:
    """Cortex proposes; our check and our role run it. One shot: the API has no repair turn of ours."""
    reply = cortex.ask(runner, question)
    state = {
        "question": question,
        "sql": reply["sql"],
        "answer": reply["text"],
        "warnings": reply["warnings"],
        "seconds": reply["seconds"],
        "request_id": reply["request_id"],
        "cost_usd": 0.0,
        "attempts": [],
    }
    if reply["sql"] is None:
        return {**state, "outcome": "refused", "refused_by": "model"}
    try:
        sql_guard.check(reply["sql"], semantic_views=frozenset({cortex.SEMANTIC_VIEW}))
    except sql_guard.Refused:
        return {**state, "outcome": "refused", "refused_by": "guard"}
    except SyntaxError as err:
        return {**state, "outcome": "gave_up", "attempts": [{"sql": reply["sql"], "error": str(err)}]}
    try:
        columns, rows, query_id = runner.run(reply["sql"])
    except graph.SqlError as err:
        return {**state, "outcome": "gave_up", "attempts": [{"sql": reply["sql"], "error": str(err)}]}
    return {**state, "outcome": "answered", "columns": columns, "rows": rows, "query_id": query_id}


def _copilot(runner):
    from dotenv import load_dotenv

    from copilot.claude import Claude

    load_dotenv(ROOT / ".env.copilot", override=False)
    schema = prompt_block(runner.columns())
    g = graph.build(Claude(), runner)
    return lambda question: graph.ask(g, question, schema)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["keys", "ask", "eval", "regrade"])
    parser.add_argument("question", nargs="?")
    parser.add_argument("--tool", choices=["copilot", "cortex"], default="copilot")
    parser.add_argument("--role", default="LEDGERLINE_ANALYST")
    parser.add_argument("--only", help="comma-separated question ids")
    args = parser.parse_args(argv)
    if args.command == "regrade":
        return regrade(Path(args.question))
    load_local_env()

    from copilot.snowflake_runner import SnowflakeRunner

    runner = SnowflakeRunner(args.role, tag=f"{args.tool}:{args.command}")
    print("signed in as", " / ".join(map(str, runner.identity)))
    try:
        questions = grade.load()
        if args.only:
            wanted = set(args.only.split(","))
            questions = [q for q in questions if q["id"] in wanted]
        if args.command == "keys":
            for qid, row in keys(runner, questions).items():
                print(qid, json.dumps(row, default=str))
            return 0
        ask = _copilot(runner) if args.tool == "copilot" else (lambda question: ask_cortex(runner, question))
        if args.command == "ask":
            if not args.question:
                parser.error("ask needs a question")
            s = ask(args.question)
            print(
                json.dumps(
                    {
                        k: s.get(k)
                        for k in (
                            "outcome",
                            "sql",
                            "reading",
                            "rows",
                            "answer",
                            "path",
                            "attempts",
                            "cost_usd",
                        )
                    },
                    indent=2,
                    default=str,
                )
            )
            return 0
        return evaluate(runner, ask, questions, args)
    finally:
        runner.close()


def regrade(path: Path) -> int:
    """Grade saved answers again with the current grader — no model asked, no Snowflake. The first
    grade is kept beside the new one (``first_grade``), so a grader fix is visible, never silent."""
    by_id = {q["id"]: q for q in grade.load()}
    lines = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
    right = 0
    for r in lines:
        g = grade.grade(by_id[r["id"]], r["key"], r)
        if g != r["grade"]:
            r.setdefault("first_grade", r["grade"])
        r["grade"] = g
        right += g["correct"]
        changed = "  (was: " + r["first_grade"]["why"] + ")" if "first_grade" in r else ""
        print(f"{r['id']:>4} {'RIGHT' if g['correct'] else 'wrong':5} {g['why']}{changed}")
    path.write_text("".join(json.dumps(r, default=str) + "\n" for r in lines), encoding="utf-8")
    print(f"\n{right}/{len(lines)} right (execution accuracy {right / len(lines):.0%}) — {path.name}")
    return 0


def evaluate(runner, ask, questions: list[dict], args) -> int:
    key = keys(runner, questions)
    OUT.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = OUT / f"{stamp}_{args.tool}_{args.role.lower()}.jsonl"
    right = cost = 0.0
    with path.open("w", encoding="utf-8") as f:
        for q in questions:
            started = time.monotonic()
            s = ask(q["question"])
            g = grade.grade(q, key.get(q["id"]), s)
            extra = (
                grade.unsupported_numbers(s.get("answer", ""), s.get("rows") or [])
                if args.tool == "copilot"
                else []
            )
            right += g["correct"]
            cost += s.get("cost_usd", 0.0)
            repairs = len(s.get("attempts") or [])
            print(
                f"{q['id']:>4} {'RIGHT' if g['correct'] else 'wrong':5} {s.get('outcome', ''):8} "
                f"repairs={repairs} "
                f"{g['why']}{'  prose numbers not in result: ' + ', '.join(extra) if extra else ''}"
            )
            f.write(
                json.dumps(
                    {
                        "id": q["id"],
                        "question": q["question"],
                        "grade": g,
                        "key": key.get(q["id"]),
                        "unsupported_numbers": extra,
                        "seconds": round(time.monotonic() - started, 1),
                        **{
                            k: s.get(k)
                            for k in (
                                "outcome",
                                "refused_by",
                                "sql",
                                "reading",
                                "query_id",
                                "columns",
                                "rows",
                                "answer",
                                "attempts",
                                "path",
                                "cost_usd",
                                "warnings",
                            )
                        },
                    },
                    default=str,
                )
                + "\n"
            )
    print(
        f"\n{args.tool} as {args.role}: {int(right)}/{len(questions)} right "
        f"(execution accuracy {right / len(questions):.0%}), Claude spend ${cost:.4f}\nwritten: {path}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
