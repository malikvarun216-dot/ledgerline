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
    assert "replace(_metadata.file_path, 's3://ledgerline-bridge-8176114c/repaired/', '')" in sql
    # compared by alias: `_metadata` inside the NOT EXISTS would be raw's own files (S29)
    assert "held.source_file = f.source_file" in sql
    assert "_metadata" not in sql.split("WHERE NOT EXISTS", 1)[1]
    assert "FROM parquet.`s3://ledgerline-bridge-8176114c/repaired/silver/review/v*/*.parquet`" in sql


def test_copy_reads_only_the_silver_tree():
    """COPY INTO must never see repaired/: it keys on paths, and the copy's path is new."""
    sql = dbx_raw.copy_sql("review")
    assert "FROM 's3://ledgerline-bridge-8176114c/silver/review/'" in sql
    assert "repaired" not in sql
