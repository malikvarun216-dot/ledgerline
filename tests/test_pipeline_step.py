"""scripts/pipeline_step.py — which queries of 12_check_raw.sql count as checks, without Snowflake."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from pipeline_step import is_check  # noqa: E402

# A cursor.description is a sequence of column descriptors whose first item is the column name;
# Snowflake returns unquoted aliases upper-cased.
CHECK = [("PROBLEM",), ("TABLE_NAME",), ("ROWS_",)]
SUMMARY = [("TABLE_NAME",), ("ROWS_",), ("INSERTS",), ("UPDATES",), ("DELETES",), ("FILES",)]


def test_a_check_query_counts():
    assert is_check(CHECK)


def test_the_summary_does_not_count():
    # S17: the summary's 8 rows (one per Silver table) were reported as 8 problems on a clean RAW.
    assert not is_check(SUMMARY)


def test_a_statement_without_rows_does_not_count():
    assert not is_check(None)
    assert not is_check([])


def test_every_select_in_the_file_is_a_check_or_the_summary():
    # A new query added to the file must either name its first column `problem` or be read as a summary
    # on purpose: exactly one SELECT in the file may lack the `problem` column.
    sql = (ROOT / "snowflake" / "12_check_raw.sql").read_text(encoding="utf-8")
    selects = re.findall(r"^(?:WITH|SELECT)\b.*?;", sql, flags=re.M | re.S)
    unlabelled = [s for s in selects if not re.search(r"'[^']*'\s+AS\s+problem\b", s, flags=re.I)]
    assert len(selects) == 6
    assert len(unlabelled) == 1 and "Summary" not in unlabelled[0] and "COUNT_IF" in unlabelled[0]
