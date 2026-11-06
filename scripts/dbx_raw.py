"""Load the bridge's Parquet files into Databricks `workspace.raw.silver_changes` — the input of dbt's
`databricks` target, as `LEDGERLINE.RAW.SILVER_CHANGES` is the input of the Snowflake one.

    python scripts/dbx_raw.py            # plan: files per Silver table in S3 vs already loaded
    python scripts/dbx_raw.py --apply    # create the table if missing, COPY INTO once per table

Why (decisions.md S28): both Golds must start from the same files, so a difference between them
comes from dbt or the platform, never the input. Databricks `COPY INTO` remembers every file it
loaded into a table (like Snowflake's load history), so a re-run loads only new files — the same
idempotency contract as Snowpipe.

One COPY per Silver table folder, not one over `silver/`: the folders have ten different schemas,
and Databricks keeps a null field inside a VARIANT (Snowflake drops it), so a merged schema would
put every other table's columns into each record. Each row becomes one VARIANT object, the whole
Parquet row (`to_variant_object(struct(*))`).

Runs as the `ledgerline-ci` service principal (OAuth M2M, .env), over the SQL Statement API on the
one SQL warehouse. Grants: databricks/gold/dbt_target_setup.sql (the owner runs it).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from query_as import api, token  # noqa: E402

from generators._common import load_local_env  # noqa: E402

BUCKET = "s3://ledgerline-bridge-8176114c/"
TABLE = "workspace.raw.silver_changes"
SILVER_TABLES = ["customer", "product", "seller", "customer_order_link", "review", "review_labels",
                 "orders", "order_items", "inventory", "inventory_events"]

CREATE = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  table_name         STRING    COMMENT 'Silver table: the folder before the v<from>-<to> folder',
  record             VARIANT   COMMENT 'the whole Parquet row; typed in dbt staging',
  source_file        STRING    COMMENT 'from the bucket: silver/<table>/v…/part-….parquet, as Snowflake',
  source_row         BIGINT    COMMENT 'row number inside that file, from 1 (as METADATA$FILE_ROW_NUMBER)',
  file_last_modified TIMESTAMP COMMENT 'when the file was written to S3',
  loaded_at          TIMESTAMP COMMENT 'when this COPY ran'
)
COMMENT 'Databricks Silver change feed as delivered by the bridge; loaded only by scripts/dbx_raw.py'
"""


def copy_sql(table: str) -> str:
    # PATTERN is relative to the FROM path. The folder name ends with '/', so `customer/` never
    # matches `customer_order_link/`.
    return f"""
COPY INTO {TABLE}
FROM (
  SELECT '{table}'                                              AS table_name,
         to_variant_object(struct(*))                           AS record,
         replace(_metadata.file_path, '{BUCKET}', '')           AS source_file,
         _metadata.row_index + 1                                AS source_row,  -- 1-based, as Snowflake's
         _metadata.file_modification_time                       AS file_last_modified,
         current_timestamp()                                    AS loaded_at
  FROM '{BUCKET}silver/{table}/'
)
FILEFORMAT = PARQUET
PATTERN = 'v*/*.parquet'
"""


class Warehouse:
    def __init__(self):
        self.host = os.environ["DATABRICKS_HOST"].rstrip("/")
        self.bearer = token(self.host, os.environ["DATABRICKS_CLIENT_ID"],
                            os.environ["DATABRICKS_CLIENT_SECRET"])
        self.id = api(self.host, self.bearer, "GET", "/api/2.0/sql/warehouses")["warehouses"][0]["id"]

    def sql(self, statement: str) -> list[dict]:
        result = api(self.host, self.bearer, "POST", "/api/2.0/sql/statements",
                     {"warehouse_id": self.id, "statement": statement, "wait_timeout": "50s"})
        while result.get("status", {}).get("state") in {"PENDING", "RUNNING"}:
            time.sleep(3)
            result = api(self.host, self.bearer, "GET", f"/api/2.0/sql/statements/{result['statement_id']}")
        if "http_error" in result or result.get("status", {}).get("state") != "SUCCEEDED":
            raise SystemExit(f"FAILED: {result.get('status', {}).get('error', result)}\n{statement}")
        cols = [c["name"] for c in result["manifest"]["schema"].get("columns", [])]
        data = result.get("result", {}).get("data_array", []) or []
        return [dict(zip(cols, row, strict=True)) for row in data]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="create the table and run the COPYs")
    args = parser.parse_args()
    load_local_env()
    wh = Warehouse()

    if args.apply:
        wh.sql(CREATE)
    exists = wh.sql("SHOW TABLES IN workspace.raw LIKE 'silver_changes'")
    loaded = {}
    if exists:
        loaded = {r["table_name"]: (int(r["files"]), int(r["n"])) for r in wh.sql(
            f"SELECT table_name, count(DISTINCT source_file) AS files, count(*) AS n "
            f"FROM {TABLE} GROUP BY 1")}

    for table in SILVER_TABLES:
        before = loaded.get(table, (0, 0))
        if not args.apply:
            print(f"  {table:<20} held: {before[0]:>3} files, {before[1]:>8} rows")
            continue
        out = wh.sql(copy_sql(table))
        print(f"  {table:<20} COPY: {out}")
    if args.apply:
        print("\nafter:")
        for r in wh.sql(f"SELECT table_name, count(DISTINCT source_file) AS files, count(*) AS n "
                        f"FROM {TABLE} GROUP BY 1 ORDER BY 1"):
            print(f"  {r['table_name']:<20} {r['files']:>3} files, {r['n']:>8} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
