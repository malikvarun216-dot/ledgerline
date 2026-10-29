"""The analyst copilot, offline (Session 24): the graph's branches, the read-only check, the grader.

No Claude and no Snowflake: a fake model returns scripted proposals, a fake runner returns scripted
rows or errors. What is proven here is the shape of the work — a failing query is repaired at most
MAX_REPAIRS times, a write request never reaches the runner, a decline ends the run — and that the
grader marks a trap as a trap.
"""

from __future__ import annotations

import pytest

from copilot import grade, graph, sql_guard
from copilot.claude import Proposal
from copilot.facts import TABLES, prompt_block


class FakeLLM:
    def __init__(self, proposals):
        self.proposals = list(proposals)
        self.seen_attempts = []

    def write_sql(self, question, schema, attempts):
        self.seen_attempts.append(list(attempts))
        sql = self.proposals.pop(0)
        if sql.startswith("DECLINE"):
            return Proposal(None, "", sql, 0.01)
        return Proposal(sql, "all orders", None, 0.01)

    def answer(self, question, sql, reading, columns, rows):
        return f"The answer is {rows[0][0]}.", 0.002


class FakeRunner:
    def __init__(self, results):
        self.results = list(results)
        self.ran = []

    def run(self, sql):
        self.ran.append(sql)
        r = self.results.pop(0)
        if isinstance(r, Exception):
            raise r
        return ["N"], r, "01b-fake"


GOOD = "SELECT COUNT(DISTINCT customer_unique_id) FROM LEDGERLINE.GOLD.FCT_ORDERS"


def _run(proposals, results):
    llm, runner = FakeLLM(proposals), FakeRunner(results)
    state = graph.ask(graph.build(llm, runner), "How many customers?", "schema")
    return state, llm, runner


def test_happy_path_writes_checks_runs_answers():
    s, _, runner = _run([GOOD], [[[96096]]])
    assert s["outcome"] == "answered"
    assert s["path"] == ["write_sql", "check", "run_sql", "answer"]
    assert runner.ran == [GOOD] and s["rows"] == [[96096]] and s["query_id"] == "01b-fake"
    assert s["cost_usd"] == pytest.approx(0.012)


def test_snowflake_error_goes_back_to_the_model_and_is_repaired():
    bad = "SELECT COUNT(DISTINCT customer_unique_idd) FROM LEDGERLINE.GOLD.FCT_ORDERS"
    s, llm, _ = _run(
        [bad, GOOD], [graph.SqlError("000904 invalid identifier 'CUSTOMER_UNIQUE_IDD'"), [[96096]]]
    )
    assert s["outcome"] == "answered"
    assert s["path"] == ["write_sql", "check", "run_sql", "write_sql", "check", "run_sql", "answer"]
    assert llm.seen_attempts[1] == [{"sql": bad, "error": "000904 invalid identifier 'CUSTOMER_UNIQUE_IDD'"}]


def test_repairs_are_capped():
    err = graph.SqlError("boom")
    n = graph.MAX_REPAIRS + 1
    s, _, runner = _run([GOOD] * n, [err] * n)
    assert s["outcome"] == "gave_up" and len(runner.ran) == n and len(s["attempts"]) == n
    assert s["path"][-1] == "give_up"


def test_same_sql_after_a_failure_is_still_run_again():
    # The failure flag is per step, not "this SQL failed before": a retry of identical SQL is a new attempt.
    s, _, runner = _run([GOOD, GOOD], [graph.SqlError("transient"), [[1]]])
    assert s["outcome"] == "answered" and len(runner.ran) == 2


def test_a_write_never_reaches_snowflake_and_is_not_repaired():
    s, llm, runner = _run(["DELETE FROM LEDGERLINE.GOLD.FCT_ORDERS WHERE status = 'canceled'"], [])
    assert s["outcome"] == "refused" and s["refused_by"] == "guard"
    assert runner.ran == [] and len(llm.seen_attempts) == 1
    assert s["answer"].startswith("Refused:")


def test_a_model_decline_ends_the_run():
    s, _, runner = _run(["DECLINE: this asks to delete data"], [])
    assert s["outcome"] == "refused" and s["refused_by"] == "model" and runner.ran == []


def test_unparseable_sql_counts_as_a_failed_attempt():
    s, _, runner = _run(["SELEC 1 FRM", GOOD], [[[1]]])
    assert s["outcome"] == "answered" and runner.ran == [GOOD] and len(s["attempts"]) == 1


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1; DROP TABLE LEDGERLINE.GOLD.FCT_ORDERS",
        "SELECT * FROM LEDGERLINE.RAW.SILVER_CHANGES",
        "SELECT * FROM LEDGERLINE.S18.ANYTHING",
        "SELECT * FROM OTHERDB.GOLD.FCT_ORDERS",
        "CREATE TABLE LEDGERLINE.GOLD.X AS SELECT 1",
        "MERGE INTO LEDGERLINE.GOLD.FCT_ORDERS t USING (SELECT 1 a) s ON 1=1 WHEN MATCHED THEN DELETE",
        "SHOW TABLES",
        "CALL SYSTEM$WAIT(1)",
        "GRANT SELECT ON TABLE LEDGERLINE.GOLD.FCT_ORDERS TO ROLE PUBLIC",
    ],
)
def test_guard_refuses(sql):
    with pytest.raises(sql_guard.Refused):
        sql_guard.check(sql)


@pytest.mark.parametrize(
    "sql",
    [
        GOOD,
        "WITH p AS (SELECT customer_unique_id FROM LEDGERLINE.GOLD.FCT_ORDERS) SELECT COUNT(*) FROM p",
        "SELECT 1 FROM GOLD.FCT_ORDERS UNION ALL SELECT 2 FROM FCT_ORDER_ITEMS",
    ],
)
def test_guard_allows_reads_of_gold(sql):
    assert sql_guard.check(sql) == sql


def test_guard_allows_cortex_semantic_view_only_when_named():
    sql = (
        "SELECT * FROM SEMANTIC_VIEW(LEDGERLINE.S24.GOLD_SV METRICS COUNT(DISTINCT scorecard.seller_id) AS n "
        "WHERE scorecard.tier = 'at risk')"
    )
    with pytest.raises(sql_guard.Refused):
        sql_guard.check(sql)  # the copilot may not read S24
    assert sql_guard.check(sql, semantic_views=frozenset({"LEDGERLINE.S24.GOLD_SV"})) == sql
    with pytest.raises(sql_guard.Refused):
        sql_guard.check(
            "SELECT * FROM LEDGERLINE.S24.OTHER", semantic_views=frozenset({"LEDGERLINE.S24.GOLD_SV"})
        )


def test_every_question_is_well_formed():
    qs = grade.load()
    assert len(qs) == 20 and len({q["id"] for q in qs}) == 20
    for q in qs:
        assert q["kind"] in {"value", "top", "masked", "refuse"}
        if q["kind"] in {"value", "top"}:
            assert q["accept"] and "key" in q
            sql_guard.check(q["key"])  # keys are reads of Gold too
        for col in q.get("traps") or {}:
            assert col.lower() in q["key"].lower()


def _q(qid):
    return next(q for q in grade.load() if q["id"] == qid)


def test_grade_right_reading_and_trap():
    q, key = _q("Q1"), {"people_who_ordered": 96096, "customer_ids": 99441}
    assert grade.grade(q, key, {"outcome": "answered", "rows": [[96096]]})["correct"]
    g = grade.grade(q, key, {"outcome": "answered", "rows": [[99441]]})
    assert not g["correct"] and g["why"].startswith("trap: counted customer_id")


def test_grade_share_as_percent_and_both_numbers_of_a_reading():
    q8 = _q("Q8")
    assert grade.grade(
        q8,
        {"late_share": 0.067736, "late_over_all_orders": 0.0657},
        {"outcome": "answered", "rows": [[6.77]]},
    )["correct"]
    q10 = _q("Q10")
    key = {
        "late_avg": 2.2709,
        "on_time_avg": 4.2901,
        "late_avg_orders": 2.27,
        "on_time_avg_orders": 4.29,
        "late_avg_of_avgs": 1.9,
        "on_time_avg_of_avgs": 3.6,
    }
    assert not grade.grade(q10, key, {"outcome": "answered", "rows": [[True, 2.2709]]})[
        "correct"
    ]  # one half only
    assert grade.grade(q10, key, {"outcome": "answered", "rows": [[True, 2.2709], [False, 4.2901]]})[
        "correct"
    ]


def test_grade_top_reads_the_first_row_only():
    q, key = _q("Q3"), {"top_state": "SP", "people": 40143}
    assert grade.grade(q, key, {"outcome": "answered", "rows": [["SP", 40143], ["RJ", 12000]]})["correct"]
    assert not grade.grade(q, key, {"outcome": "answered", "rows": [["RJ", 12000], ["SP", 40143]]})["correct"]


def test_grade_masked_refuse_and_too_broad():
    assert grade.grade(_q("Q18"), {}, {"outcome": "answered", "rows": [["***", 95696]]})["correct"]
    assert not grade.grade(
        _q("Q20"), {}, {"outcome": "answered", "rows": [[0]], "answer": "No review mentions it."}
    )["correct"]
    assert grade.grade(
        _q("Q20"), {}, {"outcome": "answered", "rows": [[0]], "answer": "The text is masked for this role."}
    )["correct"]
    assert (
        grade.grade(_q("Q19"), None, {"outcome": "refused", "refused_by": "guard"})["why"]
        == "stopped by guard"
    )
    rows = [[i] for i in range(11)] + [[175]]
    assert not grade.grade(
        _q("Q12"), {"sellers": 175, "seller_months": 281}, {"outcome": "answered", "rows": rows}
    )["correct"]


def test_unsupported_numbers_allows_rounding_but_flags_inventions():
    rows = [[0.067736, 96476]]
    assert grade.unsupported_numbers("6.8% of 96,476 delivered orders were late.", rows) == []
    assert grade.unsupported_numbers("6.8% of 97,000 orders were late.", rows) == ["97,000"]


def test_prompt_offers_only_tables_the_role_can_see():
    text = prompt_block({"FCT_ORDERS": [("ORDER_ID", "TEXT")]})
    assert "FCT_ORDERS" in text and "FCT_REVIEWS" not in text
    assert set(TABLES) >= {"FCT_ORDERS", "DELIVERY_PROMISE", "LATE_VS_RATING"}


def test_grade_percent_key_matches_a_share_answer_and_saved_text_numbers():
    # S24: Cortex returned 0.0560732 for a key of 5.607 (%), and a saved run holds numbers as text.
    q, key = _q("Q7"), {"repeat_share_pct": "5.60731990", "repeat_within_2018_pct": "4.51466747"}
    assert grade.grade(q, key, {"outcome": "answered", "rows": [["0.05607320", "166099.72"]]})["correct"]
    g = grade.grade(q, key, {"outcome": "answered", "rows": [["0.0451466"]]})
    assert not g["correct"] and g["why"].startswith("trap:")


def test_grade_masked_accepts_a_decline_that_names_the_mask():
    s = {
        "outcome": "refused",
        "refused_by": "model",
        "answer": "The comment text is masked ('***') for readers.",
    }
    assert grade.grade(_q("Q20"), {}, s)["correct"]
    assert not grade.grade(_q("Q20"), {}, {"outcome": "refused", "answer": "Cannot answer."})["correct"]
