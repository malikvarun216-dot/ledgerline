"""Inventory CDC — the fast-changing source where only current state matters.

What it does
------------
Emits a Debezium-style change log for stock held per ``(product_id, seller_id)``:

    op='I'  seed         one per SKU, before any order exists
    op='U'  sale         stock goes DOWN, driven by real olist_order_items rows
    op='U'  restock      stock goes UP
    op='D'  delist       the SKU stops being carried

Why it is coupled to the order stream
-------------------------------------
Every decrement traces back to a real ``order_items`` row rather than to
synthetic jitter. That coupling is the only reason the cross-source
reconciliation test means anything: units sold in Gold must equal the sum of
negative stock deltas in the CDC feed. If the decrements were random noise
running alongside the pipeline, "reconciliation" would be comparing two
unrelated numbers.

Three invariants this generator must hold
-----------------------------------------
**1. Negative delta implies a sale, always.** Restocks are strictly positive and
seeds have no predecessor, so ``-sum(delta where delta < 0)`` is exactly units
sold. Any other downward movement (a manual correction, say) would break the
tie-out, so this generator never emits one.

**2. Stock never clamps at zero.** Initial stock is seeded above lifetime
demand. A clamp would swallow a decrement, and the reconciliation would fail
for a reason that has nothing to do with the pipeline under test.

**3. seq is monotonic in event time and survives a restart.** It is *derived*
from the event timestamp rather than counted, so there is no counter to persist
and nothing to reset. See :func:`assign_seq`.

What "units sold" counts
------------------------
Units **ordered**, including orders later canceled. A cancellation would restock
in a real system; modelling that would put a positive delta where the tie-out
expects none. The definition is stated here rather than discovered later.

How it fits
-----------
Produces to Confluent topic ``inventory.cdc``. Bronze appends the raw op log and
does **not** interpret ops. Silver MERGEs on op flags with in-batch dedup and a
``seq`` guard (Session 9); Session 10 removes each guard in turn to watch it
break.

Usage
-----
    python generators/inventory_cdc.py --limit 500
    python generators/inventory_cdc.py --out-of-order-pct 15   # Session 10 material
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from generators import olist
from generators._common import (
    ASSUMED_TZ,
    JsonlSink,
    MessageSink,
    ReplayClock,
    deterministic_id,
    envelope,
    load_local_env,
)

SOURCE = "inventory_cdc"
DEFAULT_TOPIC = "inventory.cdc"

OP_INSERT = "I"
OP_UPDATE = "U"
OP_DELETE = "D"

# Reason is carried for *readability in the console*, not for the pipeline to
# depend on. A real CDC feed has no such column; Silver must work off op + seq
# + the before/after images alone, and the reconciliation works off delta sign.
REASON_SEED = "seed"
REASON_SALE = "sale"
REASON_RESTOCK = "restock"
REASON_DELIST = "delist"

AVRO_SCHEMA: dict[str, Any] = {
    "type": "record",
    "name": "InventoryChange",
    "namespace": "ledgerline.inventory",
    "doc": "One change to the stock row for a (product, seller) SKU.",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "event_ts", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "produced_at", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "source", "type": "string"},
        {"name": "schema_version", "type": "int"},
        {"name": "op", "type": "string", "doc": "I | U | D"},
        {
            "name": "seq",
            "type": "long",
            "doc": "monotonic in event time; Silver rejects s.seq <= t.seq",
        },
        {"name": "sku_key", "type": "string", "doc": "product_id|seller_id"},
        {"name": "product_id", "type": "string"},
        {"name": "seller_id", "type": "string"},
        {"name": "stock_qty", "type": "int", "doc": "after-image"},
        {
            "name": "prev_stock_qty",
            "type": ["null", "int"],
            "default": None,
            "doc": "before-image; null on the seed insert",
        },
        {"name": "change_reason", "type": "string", "doc": "diagnostics only; not a pipeline input"},
    ],
}


def sku_key(product_id: str, seller_id: str) -> str:
    """Composite key, kept human-readable on purpose.

    A hash would be shorter but unreadable in the Confluent topic viewer and in
    ``DESCRIBE HISTORY`` output — and looking at those consoles is half of how
    this project is meant to be learned.
    """
    return f"{product_id}|{seller_id}"


def assign_seq(events: list[dict[str, Any]]) -> None:
    """Derive ``seq`` from event time, in place.

    ``seq = event_ts_micros * 1000 + index_within_that_microsecond``

    Derived rather than counted, which buys three things at once:

    * **restart safety** — no counter to persist, so a generator restart cannot
      reset ``seq`` and let an old event overwrite a newer one. This is a named
      danger zone in CLAUDE.md;
    * **determinism** — a replay produces identical ``seq`` values;
    * **monotonicity in event time** — which is precisely what the Silver guard
      ``WHEN MATCHED AND s.seq > t.seq`` relies on.

    Olist timestamps are second-granularity, so the low 1000 slots are free for
    tie-breaking. At 2018 dates this stays well inside int64.
    """
    events.sort(key=lambda e: (e["event_ts"], e["sku_key"], e["change_reason"]))
    within = 0
    previous_ts: int | None = None
    for event in events:
        if event["event_ts"] != previous_ts:
            within = 0
            previous_ts = event["event_ts"]
        else:
            within += 1
        if within >= 1000:
            raise RuntimeError(
                f"more than 1000 events at {event['event_ts']} — widen the seq tiebreaker"
            )
        event["seq"] = event["event_ts"] * 1000 + within


def _record(
    *,
    op: str,
    product_id: str,
    seller_id: str,
    event_ts,
    stock_qty: int,
    prev_stock_qty: int | None,
    reason: str,
    produced_at=None,
) -> dict[str, Any]:
    key = sku_key(product_id, seller_id)
    timestamp = event_ts.to_pydatetime() if hasattr(event_ts, "to_pydatetime") else event_ts
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=ASSUMED_TZ)

    record = envelope(
        deterministic_id(SOURCE, key, op, reason, timestamp.isoformat(), stock_qty),
        timestamp,
        SOURCE,
        produced_at,
    )
    record.update(
        {
            "op": op,
            "seq": 0,  # replaced by assign_seq once the full set is known
            "sku_key": key,
            "product_id": product_id,
            "seller_id": seller_id,
            "stock_qty": int(stock_qty),
            "prev_stock_qty": None if prev_stock_qty is None else int(prev_stock_qty),
            "change_reason": reason,
        }
    )
    return record


def build_cdc_events(
    order_items: pd.DataFrame,
    orders: pd.DataFrame,
    *,
    headroom: int = 2,
    buffer_stock: int = 10,
    restock_every: int = 3,
    restock_qty: int = 25,
    delist_count: int = 0,
    seed: int = 20260919,
    produced_at=None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Build the whole change log, sorted by event time with ``seq`` assigned.

    Sales are grouped to one event per ``(order, SKU)``: a three-unit line
    produces a single stock movement of -3, which is how a real system writes
    it. Emitting three separate -1 events would inflate the feed and make the
    in-batch dedup exercise artificially easy.
    """
    sales = order_items.merge(
        orders[["order_id", "order_purchase_timestamp"]], on="order_id", how="inner"
    )
    grouped = (
        sales.groupby(["product_id", "seller_id", "order_id", "order_purchase_timestamp"], as_index=False)
        .size()
        .rename(columns={"size": "qty"})
        .sort_values(["product_id", "seller_id", "order_purchase_timestamp", "order_id"], kind="stable")
    )

    stats = {
        "skus": 0,
        "units_sold": int(grouped["qty"].sum()),
        "seeds": 0,
        "sales": 0,
        "restocks": 0,
        "delists": 0,
    }
    if grouped.empty:
        return [], stats

    events: list[dict[str, Any]] = []
    final_stock: dict[str, tuple[str, str, int, Any]] = {}

    for (product_id, seller_id), sku_sales in grouped.groupby(["product_id", "seller_id"], sort=True):
        # Seed a day before THIS SKU's own first sale, not before the
        # dataset's global first order. A seller lists a product when they
        # start selling it, not on day one of the whole marketplace.
        #
        # Seeding every SKU at one shared instant is also a correctness bug,
        # not just an unrealistic model: at real Olist scale there are
        # ~34,000 distinct SKUs, all landing on the exact same microsecond,
        # which blows straight through assign_seq's 1000-per-microsecond
        # tiebreaker. See docs/incidents.md [2026-09-19] "34k SKUs collided
        # at one seed timestamp".
        seed_ts = sku_sales["order_purchase_timestamp"].min() - timedelta(days=1)
        lifetime = int(sku_sales["qty"].sum())
        # Above lifetime demand by construction, so stock never reaches zero and
        # never needs clamping. A clamp would swallow a decrement.
        initial = lifetime * headroom + buffer_stock

        events.append(
            _record(
                op=OP_INSERT,
                product_id=product_id,
                seller_id=seller_id,
                event_ts=seed_ts,
                stock_qty=initial,
                prev_stock_qty=None,
                reason=REASON_SEED,
                produced_at=produced_at,
            )
        )
        stats["seeds"] += 1
        stats["skus"] += 1

        running = initial
        last_ts = seed_ts
        for count, sale in enumerate(sku_sales.itertuples(index=False), start=1):
            events.append(
                _record(
                    op=OP_UPDATE,
                    product_id=product_id,
                    seller_id=seller_id,
                    event_ts=sale.order_purchase_timestamp,
                    stock_qty=running - sale.qty,
                    prev_stock_qty=running,
                    reason=REASON_SALE,
                    produced_at=produced_at,
                )
            )
            running -= sale.qty
            last_ts = sale.order_purchase_timestamp
            stats["sales"] += 1

            if restock_every and count % restock_every == 0:
                restock_ts = sale.order_purchase_timestamp + timedelta(hours=1)
                events.append(
                    _record(
                        op=OP_UPDATE,
                        product_id=product_id,
                        seller_id=seller_id,
                        event_ts=restock_ts,
                        stock_qty=running + restock_qty,
                        prev_stock_qty=running,
                        reason=REASON_RESTOCK,
                        produced_at=produced_at,
                    )
                )
                running += restock_qty
                last_ts = restock_ts
                stats["restocks"] += 1

        if running < 0:  # pragma: no cover - prevented by construction
            raise RuntimeError(
                f"{sku_key(product_id, seller_id)} went negative ({running}); raise --headroom"
            )
        final_stock[sku_key(product_id, seller_id)] = (product_id, seller_id, running, last_ts)

    if delist_count > 0:
        # After every sale, so a delist can never orphan a later decrement.
        delist_ts = grouped["order_purchase_timestamp"].max() + timedelta(days=1)
        rng = random.Random(f"{seed}:delist")
        for key in rng.sample(sorted(final_stock), k=min(delist_count, len(final_stock))):
            product_id, seller_id, stock, _ = final_stock[key]
            events.append(
                _record(
                    op=OP_DELETE,
                    product_id=product_id,
                    seller_id=seller_id,
                    event_ts=delist_ts,
                    stock_qty=stock,
                    prev_stock_qty=stock,
                    reason=REASON_DELIST,
                    produced_at=produced_at,
                )
            )
            stats["delists"] += 1

    assign_seq(events)
    stats["events"] = len(events)
    return events, stats


def units_sold_from_cdc(events: list[dict[str, Any]]) -> dict[str, int]:
    """Reconstruct units sold per SKU from nothing but the change log.

    Deliberately reads only ``prev_stock_qty``/``stock_qty`` — no
    ``change_reason``, no join back to orders. That is the same information a
    Silver consumer has, so if this agrees with the order stream the
    reconciliation is a real cross-source check rather than a restatement.
    """
    sold: dict[str, int] = {}
    for event in events:
        if event["prev_stock_qty"] is None or event["op"] == OP_DELETE:
            continue
        delta = event["stock_qty"] - event["prev_stock_qty"]
        if delta < 0:
            sold[event["sku_key"]] = sold.get(event["sku_key"], 0) - delta
    return sold


def shuffle_out_of_order(
    events: list[dict[str, Any]], percent: float, seed: int = 20260919
) -> list[dict[str, Any]]:
    """Disturb *emission* order while leaving ``seq`` untouched.

    This is the Session 10 experiment's raw material: the feed still carries the
    correct ordering information, so a Silver MERGE with a ``seq`` guard is
    unaffected and one without it clobbers newer state with older. Sorting the
    events by ``seq`` must always restore the true order.
    """
    if percent <= 0 or len(events) < 2:
        return list(events)

    shuffled = list(events)
    rng = random.Random(f"{seed}:ooo")
    swaps = int(len(shuffled) * percent / 100)
    for _ in range(swaps):
        i = rng.randrange(len(shuffled) - 1)
        shuffled[i], shuffled[i + 1] = shuffled[i + 1], shuffled[i]
    return shuffled


def emit(
    events: list[dict[str, Any]],
    sink: MessageSink,
    topic: str = DEFAULT_TOPIC,
    clock: ReplayClock | None = None,
) -> int:
    """Send to the sink keyed by ``sku_key``.

    Keying on the SKU puts every change for one stock row on one partition, so
    ordering is guaranteed exactly where it matters. Keying on ``event_id``
    would scatter a SKU's history across partitions and make the ``seq`` guard
    load-bearing for correctness rather than a belt-and-braces check.
    """
    from generators._common import from_micros

    for event in events:
        if clock is not None and clock.paced:
            clock.sleep_until(from_micros(event["event_ts"]))
        sink.send(topic, event["sku_key"], event)
    sink.flush()
    return len(events)


def main() -> int:
    load_local_env()  # .env is not read automatically -- see _common.load_local_env
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw-dir", type=Path, default=olist.DEFAULT_RAW_DIR)
    parser.add_argument("--out-dir", type=Path, default=Path("data/streams"))
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--sink", choices=("jsonl", "kafka"), default="jsonl")
    parser.add_argument("--limit", type=int, default=None, help="first N orders by purchase time")
    parser.add_argument("--headroom", type=int, default=2, help="seed stock multiple of lifetime demand")
    parser.add_argument("--restock-every", type=int, default=3, help="0 disables restocks")
    parser.add_argument("--restock-qty", type=int, default=25)
    parser.add_argument("--delist-count", type=int, default=0)
    parser.add_argument("--out-of-order-pct", type=float, default=0.0, help="Session 10 experiment")
    parser.add_argument("--speedup", type=float, default=0.0)
    parser.add_argument("--seed", type=int, default=20260919)
    parser.add_argument("--print-schema", action="store_true")
    args = parser.parse_args()

    if args.print_schema:
        print(json.dumps(AVRO_SCHEMA, indent=2))
        return 0

    orders = olist.load("orders", args.raw_dir).sort_values("order_purchase_timestamp", kind="stable")
    if args.limit:
        orders = orders.head(args.limit)
    items = olist.load("order_items", args.raw_dir)
    items = items[items["order_id"].isin(set(orders["order_id"]))]

    events, stats = build_cdc_events(
        items,
        orders,
        headroom=args.headroom,
        restock_every=args.restock_every,
        restock_qty=args.restock_qty,
        delist_count=args.delist_count,
        seed=args.seed,
    )

    reconstructed = sum(units_sold_from_cdc(events).values())
    ordering = shuffle_out_of_order(events, args.out_of_order_pct, args.seed)

    if args.sink == "kafka":
        from generators._common import KafkaAvroSink

        sink: MessageSink = KafkaAvroSink(
            {args.topic: json.dumps(AVRO_SCHEMA)}, client_id=f"ledgerline-{SOURCE}"
        )
    else:
        sink = JsonlSink(args.out_dir)

    clock = None
    if args.speedup > 0 and events:
        from generators._common import from_micros

        clock = ReplayClock(business_start=from_micros(events[0]["event_ts"]), speedup=args.speedup)

    try:
        sent = emit(ordering, sink, args.topic, clock)
    finally:
        sink.close()

    print(f"  SKUs (product x seller)  {stats['skus']:>9,}")
    print(f"  seed inserts   op=I      {stats['seeds']:>9,}")
    print(f"  sales          op=U      {stats['sales']:>9,}")
    print(f"  restocks       op=U      {stats['restocks']:>9,}")
    print(f"  delists        op=D      {stats['delists']:>9,}")
    print(f"  events emitted           {sent:>9,}  -> topic {args.topic!r}")
    print()
    print(f"  units sold per order_items        {stats['units_sold']:>9,}")
    print(f"  units sold rebuilt from CDC deltas{reconstructed:>9,}  <- must match")
    if args.out_of_order_pct:
        print(f"\n  emission order disturbed by {args.out_of_order_pct}% — seq values are still correct")
    return 0 if reconstructed == stats["units_sold"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
