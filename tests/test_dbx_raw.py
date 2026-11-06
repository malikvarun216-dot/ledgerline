"""dbx_raw.py: the SQL it sends, checked offline (decisions.md S28, S29)."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import dbx_raw


def test_repaired_file_is_keyed_on_the_original_name():
    """The copy under repaired/ must land with the original's source_file, and only if raw does not
    already hold that file — else a live raw would get every row twice."""
    sql = dbx_raw.repaired_insert_sql("review")
    source = "replace(_metadata.file_path, 's3://ledgerline-bridge-8176114c/repaired/', '')"
    assert sql.count(source) == 2  # stored, and used in the NOT EXISTS
    assert "WHERE NOT EXISTS" in sql
    assert "held.source_file = " + source in sql
    assert "FROM parquet.`s3://ledgerline-bridge-8176114c/repaired/silver/review/`" in sql


def test_copy_reads_only_the_silver_tree():
    """COPY INTO must never see repaired/: it keys on paths, and the copy's path is new."""
    sql = dbx_raw.copy_sql("review")
    assert "FROM 's3://ledgerline-bridge-8176114c/silver/review/'" in sql
    assert "repaired" not in sql
