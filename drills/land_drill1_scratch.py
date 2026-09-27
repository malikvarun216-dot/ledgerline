"""Drill 1: put scratch files under s3://<bucket>/ledgerline/drill1/ — never the live landing path.

    python drills/land_drill1_scratch.py reviews                # D4: the raw reviews CSV, as-is
    python drills/land_drill1_scratch.py redelivery-original    # two real seller nights, copied
    python drills/land_drill1_scratch.py redelivery-corrected   # night 2017-05-02 rewritten in place

Why a scratch prefix: the re-delivery attack needs *fabricated* business content (a "corrected"
seller city). Injecting invented facts into the live landing zone would corrupt the history
Silver and SCD2 are built from, and production game days inject faults, not fake data. The
duplicates put on `orders` were different: the same real events sent twice.

Runs as the scoped `ledgerline-dev` user from `.env` (never the ambient admin profile), and
refuses any key outside `ledgerline/drill1/`.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from generators._common import _s3_client_from_env, load_local_env

BUCKET = "ledgerline-landing-dev-fffc8b65"
LIVE_DIMS = "ledgerline/dims"
SCRATCH = "ledgerline/drill1"
REVIEWS = Path("data/raw/olist_order_reviews_dataset.csv")
NIGHTS = ("2017-01-02", "2017-05-02")
CORRECTED_NIGHT = "2017-05-02"
# The five lowest seller_ids get a "corrected" city: deterministic, easy to find in a query.
CORRECTIONS = 5
SUFFIX = " (corrected)"


def seller_key(root: str, night: str) -> str:
    return f"{root}/seller/dump_date={night}/seller.csv"


def put(s3, key: str, body: bytes) -> None:
    if not key.startswith(SCRATCH + "/"):
        raise SystemExit(f"refusing to write outside {SCRATCH}/: {key}")
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    head = s3.head_object(Bucket=BUCKET, Key=key)
    print(
        f"  put {key}\n      {len(body):,} bytes, md5 {hashlib.md5(body).hexdigest()}, "
        f"ETag {head['ETag'].strip(chr(34))}, LastModified {head['LastModified']:%Y-%m-%d %H:%M:%S}"
    )


def get(s3, key: str) -> bytes:
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def reviews(s3) -> None:
    body = REVIEWS.read_bytes()  # bytes exactly as downloaded: CRLF endings, quotes and all
    put(s3, f"{SCRATCH}/reviews/reviews.csv", body)


def redelivery_original(s3) -> None:
    for night in NIGHTS:
        put(s3, seller_key(f"{SCRATCH}/redelivery", night), get(s3, seller_key(LIVE_DIMS, night)))


def redelivery_corrected(s3) -> None:
    key = seller_key(f"{SCRATCH}/redelivery", CORRECTED_NIGHT)
    rows = list(csv.DictReader(io.StringIO(get(s3, key).decode("utf-8"), newline="")))
    if any(r["seller_city"].endswith(SUFFIX) for r in rows):
        raise SystemExit("already corrected — run redelivery-original first to reset")
    fixed = sorted(rows, key=lambda r: r["seller_id"])[:CORRECTIONS]
    for r in fixed:
        r["seller_city"] += SUFFIX
    out = io.StringIO(newline="")
    writer = csv.DictWriter(out, fieldnames=list(rows[0]), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    put(s3, key, out.getvalue().encode("utf-8"))
    print(f"  {len(rows):,} rows, same count; seller_city changed for: {[r['seller_id'] for r in fixed]}")


def main() -> int:
    load_local_env()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    actions = {
        "reviews": reviews,
        "redelivery-original": redelivery_original,
        "redelivery-corrected": redelivery_corrected,
    }
    parser.add_argument("what", choices=tuple(actions))
    args = parser.parse_args()
    actions[args.what](_s3_client_from_env())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
