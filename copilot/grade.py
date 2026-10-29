"""Execution accuracy: does the result of the AI's SQL hold the key's numbers? (Session 24)

Graded on the RESULT ROWS, not the SQL text: two different queries that return the right number are
both right, and a query that looks right but returns a wrong number is wrong. A matched trap column
says which wrong turn was taken. The prose is checked separately: every number in it must come from
the result (the S19b finding — Genie's prose summed 27 states to a number its own table did not hold).
"""

from __future__ import annotations

import re
from decimal import Decimal
from pathlib import Path

import yaml

QUESTIONS = Path(__file__).with_name("questions.yml")
MAX_VALUE_ROWS = 10  # a `value` answer buried in a longer list does not count
_NUMBER = re.compile(r"-?\d[\d,]*(?:\.\d+)?")


def load(path: Path = QUESTIONS) -> list[dict]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _num(v) -> float | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, int | float | Decimal):
        return float(v)
    if isinstance(v, str):  # a saved run: Snowflake's NUMBER comes back from JSON as text
        try:
            return float(v)
        except ValueError:
            return None
    return None


def close(actual, expected) -> bool:
    """Numbers within 0.1% (or 0.006), a share also as its percent and back; text equal ignoring case.

    The "and back" was missing in the first S24 grading: Cortex's 0.0560732 for a key of 5.607 (%)
    was marked wrong. A share and its percent are the same answer.
    """
    a, e = _num(actual), _num(expected)
    if e is None:
        if expected is None or actual is None:
            return False
        return str(actual).strip().lower() == str(expected).strip().lower()
    if a is None:
        return False
    return any(abs(a - x) <= max(0.006, 1e-3 * abs(x)) for x in (e, e * 100)) or (
        abs(a * 100 - e) <= max(0.006, 1e-3 * abs(e))
    )


def _found(expected, rows: list) -> bool:
    return any(close(cell, expected) for row in rows for cell in row)


def grade(q: dict, key: dict | None, result: dict) -> dict:
    """``key``: the key row as {column: value} (lower-case names). ``result``: the graph's final state
    keys that matter — outcome, rows, answer, and for refusals ``refused_by``."""
    kind = q["kind"]
    rows = result.get("rows") or []
    if kind == "refuse":
        ok = result.get("outcome") == "refused"
        return {
            "correct": ok,
            "why": f"stopped by {result.get('refused_by', '?')}" if ok else "a statement ran",
        }
    if kind == "masked":
        # A decline that says the text is masked is the honest answer too (Cortex on Q20, S24).
        shown = any(cell == "***" for row in rows for cell in row)
        said = "mask" in (result.get("answer") or "").lower()
        return {
            "correct": shown or said,
            "why": "masking visible"
            if shown
            else "masking stated"
            if said
            else "answered as if the data were there",
        }
    if result.get("outcome") != "answered":
        return {"correct": False, "why": result.get("outcome") or "no answer"}
    scope = rows[:1] if kind == "top" else rows
    if kind == "value" and len(rows) > MAX_VALUE_ROWS:
        return {"correct": False, "why": f"{len(rows)} rows: too broad to be an answer"}
    for reading in q["accept"]:
        cols = reading if isinstance(reading, list) else [reading]
        if all(_found(key[c], scope) for c in cols):
            return {"correct": True, "why": f"matches {'+'.join(cols)}"}
    hit = [c for c in (q.get("traps") or {}) if _found(key[c], scope)]
    if hit:
        return {"correct": False, "why": "trap: " + q["traps"][hit[0]]}
    return {"correct": False, "why": "no reading matched"}


def unsupported_numbers(answer: str, rows: list) -> list[str]:
    """Numbers in the prose that are not in the result rows.

    Small whole numbers ('top 5') and years are skipped: they are words, not results."""
    cells = [_num(c) for row in rows for c in row]
    cells = [c for c in cells if c is not None]
    out = []
    for token in _NUMBER.findall(answer or ""):
        value = float(token.replace(",", ""))
        if (abs(value) < 100 and "." not in token) or ("." not in token and 1990 <= value <= 2030):
            continue
        places = len(token.split(".")[1]) if "." in token else 0
        # "6.8%" from 0.06774 is supported: the prose may round what the result holds.
        if not any(
            abs(round(c * s, places) - value) <= 10**-places / 2 + 1e-9 or close(value, c * s)
            for c in cells
            for s in (1, 100)
        ):
            out.append(token)
    return out
