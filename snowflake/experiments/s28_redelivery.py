"""Session 28: a file re-delivered under the same name to the EXTERNAL S3 stage — skipped or loaded?

    python snowflake/experiments/s28_redelivery.py

Closes the open half of coverage row "COPY INTO load history". S13 B ran it on an INTERNAL stage:
a re-`PUT` of identical bytes was loaded again. Drill 1 ran it on Auto Loader: a rewritten file was
skipped silently, new content and all. This runs it on S3, where the bridge's files really live.

Where it writes: one small CSV, `s3://ledgerline-bridge-8176114c/silver/_scratch_s28/stock.csv`.
It sits under `silver/` only because that is the one prefix the storage integration and the IAM role
allow; the Snowpipe S3 event fires only on `.parquet` (filter checked 2026-11-06), so the live pipe
never sees it, and RAW is never touched. Snowflake objects live in scratch schema LEDGERLINE.S28
(05_scratch_schemas.sql, S28 block; LOADER has CREATE STAGE there). Uploads use the laptop's AWS CLI
credentials (boto3). Safe to re-run: the table and stage are re-created and the file re-written.
Cleanup (the human): `aws s3 rm s3://ledgerline-bridge-8176114c/silver/_scratch_s28/ --recursive`.

Each step prints Snowflake's answer next to the prediction written before the run; a WRONG is the
finding, counted, and the run continues.

  R1  first COPY loads the file
  R2  the same file, untouched: skipped by load history
  R3  re-uploaded, IDENTICAL bytes (same ETag, newer LastModified): skipped?
  R4  re-uploaded, NEW content, same name (new ETag): loaded again?
  R5  what the table holds afterwards: the old rows and the new ones, side by side
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from sf import connect  # noqa: E402

from generators._common import load_local_env  # noqa: E402

BUCKET = "ledgerline-bridge-8176114c"
PREFIX = "silver/_scratch_s28/"
KEY = PREFIX + "stock.csv"
S = "LEDGERLINE.S28"
STAGE = f"{S}.S28_REDELIVERY"
TABLE = f"{S}.T_S28"

V1 = b"sku_key,qty\nA,10\nB,20\nC,30\n"
V2 = b"sku_key,qty\nA,7\nB,20\nC,30\n"  # A sold three: same name, same size, new content

results: list[tuple[str, object, object, bool]] = []


def check(label, got, predicted):
    ok = got == predicted
    results.append((label, got, predicted, ok))
    print(f"  {'ok   ' if ok else 'WRONG'} {label}: got {got!r}, predicted {predicted!r}")


def rows(cur, sql):
    cur.execute(sql)
    names = [d[0].lower() for d in cur.description or []]
    return [dict(zip(names, r, strict=True)) for r in cur.fetchall()]


def put(s3, body):
    s3.put_object(Bucket=BUCKET, Key=KEY, Body=body)
    head = s3.head_object(Bucket=BUCKET, Key=KEY)
    print(f"  put {KEY}: ETag {head['ETag']}, LastModified {head['LastModified']:%H:%M:%S}")
    return head["ETag"]


def copy(cur):
    out = rows(cur, f"COPY INTO {TABLE} (sku_key, qty, src_last_modified) FROM "
                    f"(SELECT $1, $2::INT, METADATA$FILE_LAST_MODIFIED::STRING FROM @{STAGE})")
    statuses = [r.get("status") for r in out]
    loaded = sum(int(r.get("rows_loaded") or 0) for r in out)
    print(f"  COPY answered: {statuses or out}, rows_loaded {loaded}")
    return statuses, loaded


def main():
    # Made BEFORE load_local_env, on the CLI's default profile (the admin user): .env's
    # ledgerline-dev is refused PutObject on the bridge bucket by design (only Databricks' bridge
    # credential writes it). The human chose the admin profile for this scratch prefix only.
    s3 = boto3.Session(profile_name="default").client("s3", region_name="us-east-2")
    load_local_env()
    con = connect(role="LEDGERLINE_LOADER")
    cur = con.cursor()
    cur.execute("USE WAREHOUSE LEDGERLINE_WH")

    print("== setup")
    etag1 = put(s3, V1)
    cur.execute(f"CREATE OR REPLACE STAGE {STAGE} URL = 's3://{BUCKET}/{PREFIX}' "
                "STORAGE_INTEGRATION = LEDGERLINE_BRIDGE_S3 "
                "FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)")
    cur.execute(f"CREATE OR REPLACE TABLE {TABLE} (sku_key STRING, qty INT, src_last_modified STRING, "
                "loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP())")
    print(f"  LIST: {[r['name'] for r in rows(cur, f'LIST @{STAGE}')]}")

    print("\n== R1 first COPY")
    st, n = copy(cur)
    check("R1 status", st, ["LOADED"])
    check("R1 rows", n, 3)

    print("\n== R2 same file, untouched")
    st, n = copy(cur)
    check("R2 rows (skipped by load history)", n, 0)

    print("\n== R3 re-uploaded, identical bytes")
    time.sleep(2)  # so LastModified visibly moves
    etag3 = put(s3, V1)
    check("R3 ETag unchanged", etag3 == etag1, True)
    st, n = copy(cur)
    check("R3 rows (ETag unchanged -> skipped)", n, 0)

    print("\n== R4 re-uploaded, new content, same name")
    time.sleep(2)
    etag4 = put(s3, V2)
    check("R4 ETag changed", etag4 != etag1, True)
    st, n = copy(cur)
    check("R4 status", st, ["LOADED"])
    check("R4 rows (new ETag -> loaded again)", n, 3)

    print("\n== R5 the table afterwards")
    table = rows(cur, f"SELECT sku_key, qty, COUNT(*) AS n FROM {TABLE} GROUP BY 1, 2 ORDER BY 1, 2")
    for r in table:
        print(f"  {r}")
    check("R5 total rows", sum(r["n"] for r in table), 6)
    check("R5 SKU A has both values", [r["qty"] for r in table if r["sku_key"] == "A"], [7, 10])

    hist = rows(cur, f"SELECT file_name, status, row_count, last_load_time FROM "
                     f"TABLE(LEDGERLINE.INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME => '{TABLE}', "
                     "START_TIME => DATEADD(hour, -1, CURRENT_TIMESTAMP()))) ORDER BY last_load_time")
    print("\n== COPY_HISTORY for the table")
    for r in hist:
        print(f"  {r}")
    check("R5 COPY_HISTORY loads of the one file name", len(hist), 2)

    wrong = [r for r in results if not r[3]]
    print(f"\n{len(results) - len(wrong)}/{len(results)} predictions held")
    for label, got, pred, _ in wrong:
        print(f"  WRONG {label}: got {got!r}, predicted {pred!r}")
    con.close()


if __name__ == "__main__":
    main()
