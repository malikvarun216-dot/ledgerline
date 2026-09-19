"""Nightly dimension dumps — the slowly-changing source where history matters.

What it does
------------
Writes a **full snapshot** of each dimension, once per simulated night, to a
Hive-partitioned prefix that Auto Loader can pick up:

    dims/customer/dump_date=2017-05-01/customer.csv
    dims/product/dump_date=2017-05-01/product.csv
    dims/seller/dump_date=2017-05-01/seller.csv

Full snapshot, not a delta — that is the whole point. A row that vanishes
between two nights *is* a deletion, which is what ``WHEN NOT MATCHED BY SOURCE
... DELETE`` picks up at Silver and what a plain MERGE silently misses. Session
8 builds both halves and compares them.

The two things that are easy to get wrong
-----------------------------------------
**1. dim_updated_at must be a business timestamp, not a run timestamp.**
If every row is stamped with "now" on every dump, then every row looks changed
every night, and a dbt snapshot with ``strategy='timestamp'`` opens a new SCD2
version for all ~96k customers nightly. So this generator hashes each row's
*business attributes*, compares against last night's dump, and **carries the
previous timestamp forward when nothing actually changed**.

**2. The customer dimension is keyed on customer_unique_id, not customer_id.**
Olist's ``customer_id`` is issued *per order* (~99k values); ``customer_unique_id``
is the actual person (~96k). Keying SCD2 on ``customer_id`` makes every repeat
order look like a brand-new customer and destroys the entire point of the
dimension.

Real history vs synthetic change
--------------------------------
Customers get a **real** timeline: Olist genuinely records different cities for
the same ``customer_unique_id`` across their orders, so a dump for date D shows
each person's last observation at or before D, with ``dim_updated_at`` set to
that real observation time. Nothing is invented.

Products and sellers are **static** in Olist, so their changes are synthetic and
deterministic (seeded). This asymmetry is deliberate and worth knowing: the
customer SCD2 evidence is real, the product/seller SCD2 evidence is fabricated.

Usage
-----
    python generators/dim_dumps.py --nights 6 --stride-days 120
    python generators/dim_dumps.py --sink s3 --bucket my-bucket --prefix ledgerline
"""

from __future__ import annotations

import argparse
import random
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from generators import olist
from generators._common import BlobSink, LocalBlobSink, deterministic_id

SOURCE = "dim_dumps"
ROOT_PREFIX = "dims"
UPDATED_AT = "dim_updated_at"
DUMP_DATE = "dump_date"


@dataclass(frozen=True)
class DimensionSpec:
    """One dimension's key and the attributes whose change is meaningful.

    ``attributes`` is what the change hash covers. A column left out of it can
    change without moving ``dim_updated_at`` — so the list is a statement about
    which fields are business-meaningful, not just a column dump.
    """

    name: str
    key: str
    attributes: tuple[str, ...]

    @property
    def columns(self) -> list[str]:
        return [self.key, *self.attributes]


CUSTOMER = DimensionSpec(
    name="customer",
    key="customer_unique_id",  # NOT customer_id. See the module docstring.
    attributes=("customer_zip_code_prefix", "customer_city", "customer_state"),
)
PRODUCT = DimensionSpec(
    name="product",
    key="product_id",
    attributes=(
        "product_category_name",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "product_photos_qty",
    ),
)
SELLER = DimensionSpec(
    name="seller",
    key="seller_id",
    attributes=("seller_zip_code_prefix", "seller_city", "seller_state"),
)

DIMENSIONS = {spec.name: spec for spec in (CUSTOMER, PRODUCT, SELLER)}


# --------------------------------------------------------------------------
# Customer: a real, as-of-date timeline
# --------------------------------------------------------------------------


def customer_timeline(customers: pd.DataFrame, orders: pd.DataFrame) -> pd.DataFrame:
    """One row per (person, observation), ordered in time.

    Joins the per-order ``customer_id`` rows to their order's purchase time,
    then re-keys on ``customer_unique_id``. The result is a genuine history:
    each row is "at this moment, this person was recorded as living here".
    """
    joined = customers.merge(
        orders[["customer_id", "order_purchase_timestamp"]],
        on="customer_id",
        how="inner",
    )
    joined = joined.rename(columns={"order_purchase_timestamp": "observed_at"})
    joined = joined.sort_values(["customer_unique_id", "observed_at"], kind="stable")
    return joined[[CUSTOMER.key, "observed_at", *CUSTOMER.attributes]].reset_index(drop=True)


def customer_snapshot(timeline: pd.DataFrame, as_of: datetime) -> pd.DataFrame:
    """The dimension as it stood at ``as_of``.

    A person absent from the result has not placed their first order yet — they
    do not exist in the dimension, which is different from having been deleted
    from it. Bronze sees both as "not in this file"; only the sequence of dumps
    distinguishes them.
    """
    visible = timeline[timeline["observed_at"] <= pd.Timestamp(as_of)]
    if visible.empty:
        return pd.DataFrame(columns=[*CUSTOMER.columns, UPDATED_AT])

    latest = visible.groupby(CUSTOMER.key, as_index=False).tail(1).copy()
    latest[UPDATED_AT] = latest["observed_at"]
    return (
        latest[[*CUSTOMER.columns, UPDATED_AT]]
        .sort_values(CUSTOMER.key, kind="stable")
        .reset_index(drop=True)
    )


# --------------------------------------------------------------------------
# Product / seller: deterministic synthetic change
# --------------------------------------------------------------------------


def apply_synthetic_changes(
    frame: pd.DataFrame,
    spec: DimensionSpec,
    night: int,
    rate: int,
    seed: int,
) -> tuple[pd.DataFrame, int]:
    """Mutate ``rate`` rows, chosen deterministically from (spec, night, seed).

    Deterministic so two runs of the generator produce identical dumps — the
    same property that makes the order stream replay-safe. A random change model
    would make every rerun a different dataset and quietly invalidate any
    comparison across runs.

    Prefers backfilling a null ``product_category_name`` where one exists: a
    field that was missing and later filled is the most realistic dimension
    change there is, and it is drawn from the real data rather than invented.
    """
    if rate <= 0 or frame.empty or night == 0:
        return frame, 0

    rng = random.Random(f"{seed}:{spec.name}:{night}")
    keys = sorted(frame[spec.key].tolist())
    chosen = set(rng.sample(keys, k=min(rate, len(keys))))

    changed = frame.copy()
    mask = changed[spec.key].isin(chosen)
    target = changed.loc[mask]
    if target.empty:
        return changed, 0

    if "product_category_name" in spec.attributes:
        null_category = mask & changed["product_category_name"].isna()
        changed.loc[null_category, "product_category_name"] = "categoria_backfilled"
        # every other chosen row gets a measurable physical correction
        remaining = mask & ~null_category
        changed.loc[remaining, "product_weight_g"] = (
            pd.to_numeric(changed.loc[remaining, "product_weight_g"], errors="coerce").fillna(0) + night
        ).astype("Int64")
    else:
        city_column = next(c for c in spec.attributes if c.endswith("_city"))
        changed.loc[mask, city_column] = changed.loc[mask, city_column].astype(str) + f"-r{night}"

    return changed, int(mask.sum())


MAX_DELETED_FRACTION = 0.25


def deleted_keys(
    spec: DimensionSpec,
    universe: list[str],
    night: int,
    per_night: int,
    seed: int,
    max_fraction: float = MAX_DELETED_FRACTION,
) -> set[str]:
    """Keys removed from the dump by night ``night``, cumulative.

    Computed from (spec, night, seed) rather than stored, so the generator has
    no hidden state to drift: rerunning night 4 always removes the same rows.

    A key removed here is simply **absent from the file**. That absence is the
    only signal a deletion happened, which is exactly the condition
    ``WHEN NOT MATCHED BY SOURCE ... DELETE`` exists to handle.

    ``universe`` must be **every key the dimension will ever hold**, not the
    keys present on this particular night. The customer dimension grows as
    people place their first order, so sampling from the current night's keys
    makes the "cumulative" set recompute differently on every night — the
    selection stops being deterministic and earlier nights' deletions change
    retroactively. See docs/incidents.md [2026-09-19].

    ``max_fraction`` caps the cumulative deletion set so the dump can never be
    emptied. A dimension that reaches zero rows is not a deletion test, it is a
    truncation, and it would make ``WHEN NOT MATCHED BY SOURCE DELETE`` wipe the
    Silver table for a reason unrelated to the pattern under test.
    """
    if per_night <= 0 or not universe:
        return set()

    pool = sorted(set(universe))
    ceiling = int(len(pool) * max_fraction)
    if ceiling == 0:
        return set()

    removed: set[str] = set()
    for n in range(1, night + 1):
        if len(removed) >= ceiling:
            break
        rng = random.Random(f"{seed}:delete:{spec.name}:{n}")
        available = [k for k in pool if k not in removed]
        take = min(per_night, len(available), ceiling - len(removed))
        if take <= 0:
            break
        removed.update(rng.sample(available, k=take))
    return removed


# --------------------------------------------------------------------------
# dim_updated_at carry-forward
# --------------------------------------------------------------------------


def _normalize_attribute(value: Any) -> Any:
    """Canonicalize a value before hashing, independent of its pandas dtype.

    A column's dtype depends on how the frame was built — and for the frame
    ``read_previous`` reads back, on whether pandas saw a NaN *anywhere else*
    in that column when re-parsing the CSV. Plain ``pd.read_csv`` upcasts a
    whole integer column to float64 the moment any row in it is missing, so
    the identical product weight arrives as ``225`` (int64) on a fresh load
    and ``225.0`` (float64) on the read-back of last night's dump — and
    ``str(225) != str(225.0)``, so the hash sees a change that never happened.
    Olist's real ``product_weight_g`` has scattered nulls, so this fires on
    most rows in the real dataset despite never showing up in a small fixture
    with no missing numeric values. See docs/incidents.md [2026-09-19].

    Whole-valued numbers are normalized to their integer string form
    regardless of which numeric dtype carried them.
    """
    if pd.isna(value):
        return None
    # np.int64 is NOT a subclass of Python's int (on any platform), so an
    # (int, float) check silently misses it and this fix would not fire on
    # the exact values it exists to normalize.
    if isinstance(value, (int, float, np.integer, np.floating)):
        as_float = float(value)
        if as_float.is_integer():
            return str(int(as_float))
        return repr(as_float)
    return value


def attribute_hash(row: pd.Series, spec: DimensionSpec) -> str:
    """Hash of the business attributes only — the key and timestamps excluded.

    Including ``dim_updated_at`` in the hash would be circular: the timestamp
    would change the hash, which would change the timestamp.
    """
    return deterministic_id(*(_normalize_attribute(row[attribute]) for attribute in spec.attributes))


def carry_forward_updated_at(
    current: pd.DataFrame,
    previous: pd.DataFrame | None,
    spec: DimensionSpec,
    change_stamp: datetime,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Set ``dim_updated_at`` from a real content comparison against last night.

    * unchanged row  -> keep last night's ``dim_updated_at``
    * changed row    -> ``change_stamp`` (unless the caller already supplied a
                        real business timestamp, as the customer timeline does)
    * brand-new row  -> its own ``dim_updated_at``, left as supplied

    This function is the difference between a dbt snapshot that opens a handful
    of SCD2 versions a night and one that opens ~96k.
    """
    result = current.copy()
    stats = {"rows": len(result), "changed": 0, "unchanged": 0, "new": 0}

    if UPDATED_AT not in result.columns:
        result[UPDATED_AT] = pd.Timestamp(change_stamp)

    if previous is None or previous.empty:
        stats["new"] = len(result)
        return result, stats

    previous_hash = {
        row[spec.key]: attribute_hash(row, spec) for _, row in previous.iterrows()
    }
    previous_stamp = dict(zip(previous[spec.key], previous[UPDATED_AT], strict=True))

    stamps: list[Any] = []
    for _, row in result.iterrows():
        key = row[spec.key]
        if key not in previous_hash:
            stats["new"] += 1
            stamps.append(row[UPDATED_AT])
        elif attribute_hash(row, spec) == previous_hash[key]:
            stats["unchanged"] += 1
            stamps.append(previous_stamp[key])  # <- the carry-forward
        else:
            stats["changed"] += 1
            supplied = row[UPDATED_AT]
            # A real business timestamp from the source wins over the dump date.
            if pd.notna(supplied) and pd.Timestamp(supplied) != pd.Timestamp(previous_stamp[key]):
                stamps.append(supplied)
            else:
                stamps.append(pd.Timestamp(change_stamp))

    result[UPDATED_AT] = pd.to_datetime(pd.Series(stamps, index=result.index))
    return result, stats


# --------------------------------------------------------------------------
# Dump orchestration
# --------------------------------------------------------------------------


def dump_key(dimension: str, dump_date: date) -> str:
    return f"{ROOT_PREFIX}/{dimension}/{DUMP_DATE}={dump_date.isoformat()}/{dimension}.csv"


def read_previous(sink: BlobSink, dimension: str, before: date) -> tuple[pd.DataFrame | None, date | None]:
    """Most recent dump strictly before ``before``, or (None, None).

    Reading back is what lets this generator be run one night at a time —
    without it the carry-forward would only work inside a single invocation.
    """
    import io

    prefix = f"{ROOT_PREFIX}/{dimension}/"
    candidates: list[tuple[date, str]] = []
    for key in sink.list_keys(prefix):
        marker = f"{DUMP_DATE}="
        if marker not in key:
            continue
        stamp = key.split(marker, 1)[1].split("/", 1)[0]
        try:
            parsed = date.fromisoformat(stamp)
        except ValueError:
            continue
        if parsed < before:
            candidates.append((parsed, key))

    if not candidates:
        return None, None
    latest_date, latest_key = max(candidates)
    frame = pd.read_csv(io.StringIO(sink.get_text(latest_key)))
    if UPDATED_AT in frame.columns:
        frame[UPDATED_AT] = pd.to_datetime(frame[UPDATED_AT])
    return frame, latest_date


def write_night(
    sink: BlobSink,
    spec: DimensionSpec,
    frame: pd.DataFrame,
    dump_date: date,
) -> dict[str, int]:
    """Compare against last night, stamp, and write one full snapshot."""
    previous, _ = read_previous(sink, spec.name, dump_date)
    stamped, stats = carry_forward_updated_at(
        frame, previous, spec, datetime.combine(dump_date, datetime.min.time())
    )
    stamped[DUMP_DATE] = dump_date.isoformat()
    ordered = stamped[[*spec.columns, UPDATED_AT, DUMP_DATE]]
    sink.put_text(dump_key(spec.name, dump_date), ordered.to_csv(index=False))

    if previous is not None:
        stats["deleted_vs_previous"] = len(set(previous[spec.key]) - set(frame[spec.key]))
    else:
        stats["deleted_vs_previous"] = 0
    return stats


def run(
    frames: dict[str, pd.DataFrame],
    sink: BlobSink,
    start: date,
    nights: int,
    stride_days: int,
    change_rate: int = 25,
    delete_per_night: int = 0,
    seed: int = 20260919,
) -> list[dict[str, Any]]:
    """Generate ``nights`` consecutive dumps, ``stride_days`` apart."""
    timeline = customer_timeline(frames["customers"], frames["orders"])
    report: list[dict[str, Any]] = []

    # Every key the dimension will ever hold, fixed before the first night.
    # The customer dimension grows over time, so deriving this per night would
    # make the deletion set non-deterministic. See deleted_keys.
    universes = {
        "customer": sorted(set(timeline[CUSTOMER.key])),
        "product": sorted(set(frames["products"][PRODUCT.key])),
        "seller": sorted(set(frames["sellers"][SELLER.key])),
    }

    for night in range(nights):
        dump_date = start + timedelta(days=night * stride_days)
        as_of = datetime.combine(dump_date, datetime.max.time())

        night_frames = {
            "customer": customer_snapshot(timeline, as_of),
            "product": frames["products"][list(PRODUCT.columns)].copy(),
            "seller": frames["sellers"][list(SELLER.columns)].copy(),
        }

        for name, spec in DIMENSIONS.items():
            frame = night_frames[name]

            # Delete first, then mutate the survivors. The other order lets the
            # generator report a synthetic change on a row it then removes, so
            # its own summary disagrees with the dump it wrote.
            removed = deleted_keys(spec, universes[name], night, delete_per_night, seed)
            if removed:
                frame = frame[~frame[spec.key].isin(removed)].copy()

            changed = 0
            if name != "customer":
                frame, changed = apply_synthetic_changes(frame, spec, night, change_rate, seed)

            stats = write_night(sink, spec, frame, dump_date)
            stats.update(
                {
                    "dimension": name,
                    DUMP_DATE: dump_date.isoformat(),
                    "synthetic_changes": changed,
                    "removed_cumulative": len(removed),
                }
            )
            report.append(stats)

    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw-dir", type=Path, default=olist.DEFAULT_RAW_DIR)
    parser.add_argument("--out-dir", type=Path, default=Path("data/dumps"))
    parser.add_argument("--sink", choices=("local", "s3"), default="local")
    parser.add_argument("--bucket", help="required for --sink s3")
    parser.add_argument("--prefix", default="ledgerline")
    parser.add_argument("--start-date", default=None, help="first dump date; default = first order date")
    parser.add_argument("--nights", type=int, default=6)
    parser.add_argument("--stride-days", type=int, default=120)
    parser.add_argument(
        "--change-rate", type=int, default=25, help="synthetic product/seller edits per night"
    )
    parser.add_argument(
        "--delete-per-night", type=int, default=0, help="rows dropped from the dump per night"
    )
    parser.add_argument("--seed", type=int, default=20260919)
    args = parser.parse_args()

    frames = {name: olist.load(name, args.raw_dir) for name in ("customers", "orders", "products", "sellers")}

    if args.start_date:
        start = date.fromisoformat(args.start_date)
    else:
        start = frames["orders"]["order_purchase_timestamp"].min().date()

    if args.sink == "s3":
        if not args.bucket:
            parser.error("--sink s3 requires --bucket")
        from generators._common import S3BlobSink

        sink: BlobSink = S3BlobSink(args.bucket, args.prefix)
        destination = f"s3://{args.bucket}/{args.prefix}/{ROOT_PREFIX}/"
    else:
        sink = LocalBlobSink(args.out_dir)
        destination = str(args.out_dir / ROOT_PREFIX)

    report = run(
        frames,
        sink,
        start=start,
        nights=args.nights,
        stride_days=args.stride_days,
        change_rate=args.change_rate,
        delete_per_night=args.delete_per_night,
        seed=args.seed,
    )

    header = (
        f"  {'dump_date':<12} {'dimension':<9} {'rows':>8} {'new':>7} "
        f"{'changed':>8} {'unchanged':>10} {'gone':>6}"
    )
    print(header)
    print(f"  {'-' * 12} {'-' * 9} {'-' * 8} {'-' * 7} {'-' * 8} {'-' * 10} {'-' * 6}")
    for row in report:
        print(
            f"  {row[DUMP_DATE]:<12} {row['dimension']:<9} {row['rows']:>8,} {row['new']:>7,} "
            f"{row['changed']:>8,} {row['unchanged']:>10,} {row['deleted_vs_previous']:>6,}"
        )
    print(f"\n  written to {destination}")
    print("  'unchanged' rows keep last night's dim_updated_at — that is what keeps SCD2 honest.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
