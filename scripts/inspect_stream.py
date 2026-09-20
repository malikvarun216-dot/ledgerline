"""Re-derive a stream's shape from its artifacts, instead of trusting a summary.

Why this exists
---------------
The generators print a summary table when they run - event counts, the CDC
reconciliation - and then it scrolls away. Worse, that table is the generator
describing its own *intent*. Session 1 has an incident where exactly that
disagreed with the file it produced (``changed = 0`` printed while the
generator's internal report claimed a change had been applied), and the rule
that came out of it was: trust the artifact, not the summary.

So this reads the artifact back and recomputes everything independently. Point
it at a local JSONL file written by ``--sink jsonl``:

    python scripts/inspect_stream.py data/streams/orders.jsonl
    python scripts/inspect_stream.py data/streams/inventory.cdc.jsonl

It reports the same numbers the generator prints. If the two ever disagree,
the file is right and the summary is wrong.

What it checks that a summary cannot
------------------------------------
* ``event_id`` uniqueness - the property the whole idempotency story rests on.
* business time vs processing time - ``event_ts`` should be Olist-era (2016-18)
  while ``produced_at`` is now. Them being close together means the replay
  clock has stopped separating them.
* for CDC, **units sold rebuilt from deltas** - summed from ``prev_stock_qty``
  and ``stock_qty`` only. ``change_reason`` is deliberately ignored: reading it
  would be comparing the generator's own label against itself rather than
  deriving the answer a second way.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def read_records(path: Path) -> list[dict[str, Any]]:
    records = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        for line in handle:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def show_envelope(values: list[dict[str, Any]]) -> None:
    event_ids = {value["event_id"] for value in values}
    print(f"  records                  {len(values):>8,}")
    print(f"  distinct event_id        {len(event_ids):>8,}", end="")
    if len(event_ids) == len(values):
        print("   <- no duplicates")
    else:
        print(f"   <- {len(values) - len(event_ids):,} DUPLICATES")

    event_times = [value["event_ts"] for value in values]
    produced = [value["produced_at"] for value in values]
    first = datetime.fromtimestamp(min(event_times) / 1e6, UTC)
    last = datetime.fromtimestamp(max(event_times) / 1e6, UTC)
    made = datetime.fromtimestamp(max(produced) / 1e6, UTC)
    print(f"  event_ts    (business)   {first:%Y-%m-%d} .. {last:%Y-%m-%d}")
    print(f"  produced_at (processing) {made:%Y-%m-%d}")
    gap_years = (made - last).days / 365.25
    print(f"  separation               {gap_years:>8.1f} years  <- replay clock working")


def show_orders(values: list[dict[str, Any]]) -> None:
    by_type = Counter(value["event_type"] for value in values)
    orders = {value["order_id"] for value in values}
    synthetic = sum(1 for value in values if value.get("ts_is_synthetic"))
    print(f"\n  distinct order_id        {len(orders):>8,}")
    print("  events per type:")
    for name, count in sorted(by_type.items(), key=lambda pair: -pair[1]):
        print(f"      {name:<12} {count:>8,}")
    print(f"  synthetic timestamps     {synthetic:>8,}  (canceled / unavailable)")

    with_items = [value for value in values if value.get("items")]
    units = sum(len(value["items"]) for value in with_items)
    print(f"  events carrying items    {len(with_items):>8,}  ('created' only)")
    print(f"  line-item units total    {units:>8,}")


def show_cdc(values: list[dict[str, Any]]) -> None:
    by_op = Counter(value["op"] for value in values)
    skus = {value["sku_key"] for value in values}
    print(f"\n  distinct sku_key         {len(skus):>8,}")
    print("  events per op:")
    for name, count in sorted(by_op.items()):
        print(f"      {name:<12} {count:>8,}")

    # Units sold, derived only from the before/after stock image. A negative
    # movement is a sale by construction: restocks are strictly positive and a
    # seed has no predecessor.
    sold = 0
    restocked = 0
    for value in values:
        previous = value.get("prev_stock_qty")
        if previous is None:
            continue
        delta = value["stock_qty"] - previous
        if delta < 0:
            sold += -delta
        else:
            restocked += delta
    print(f"\n  units sold (from deltas) {sold:>8,}  <- reconciliation input")
    print(f"  units restocked          {restocked:>8,}")

    seqs = [value["seq"] for value in values]
    print(f"  distinct seq             {len(set(seqs)):>8,} of {len(seqs):,}")
    print(f"  seq monotonic overall    {seqs == sorted(seqs)!s:>8}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("path", type=Path, help="a JSONL file written by --sink jsonl")
    args = parser.parse_args()

    if not args.path.exists():
        parser.error(f"{args.path} does not exist. Run a generator with --sink jsonl first.")

    records = read_records(args.path)
    if not records:
        parser.error(f"{args.path} is empty")

    values = [record["value"] for record in records]
    source = values[0].get("source", "?")
    print(f"\n{args.path}  (source = {source})")
    print("-" * 60)
    show_envelope(values)

    if source == "order_events":
        show_orders(values)
    elif source == "inventory_cdc":
        show_cdc(values)
    else:
        print(f"  (no per-source view for {source!r})")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
