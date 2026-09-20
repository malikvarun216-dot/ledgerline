"""Order event generator — the append-only, immutable-fact stream.

What it does
------------
Fans each Olist order row out into the sequence of events that actually
happened to it, one event per real timestamp on the row:

    order_purchase_timestamp      -> created    (carries the line items)
    order_approved_at             -> approved
    order_delivered_carrier_date  -> shipped
    order_delivered_customer_date -> delivered

plus a terminal ``canceled`` / ``unavailable`` event for orders whose final
status is one the timestamps cannot express.

Why fan out at all
------------------
A one-event-per-order feed would give Silver's ``MERGE INTO ... ON order_id``
nothing to do — every row would be an insert, and the MERGE would be an insert
in disguise. Fanning out means an order arrives, then *updates* three more
times, which is what makes the MERGE, the idempotency, and the eventual
late-arrival experiment real rather than decorative.

Why these timestamps specifically
---------------------------------
They are the one thing in Olist that cannot be synthesized convincingly: a
real status lifecycle with real, uneven gaps. This is the reason the dataset
was chosen (docs/decisions.md, "Olist plus TPC-DS").

How it fits
-----------
Produces to Confluent topic ``orders``. Bronze appends it exactly-once using
Kafka offsets as ``txnVersion``; Silver MERGEs on ``order_id``; Gold builds
``fct_orders`` incrementally. The ``items`` array on ``created`` is also what
the inventory CDC feed decrements against, which is what makes the
cross-source reconciliation test possible.

Usage
-----
    python generators/order_events.py --limit 500            # local JSONL
    python generators/order_events.py --sink kafka --speedup 2592000
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import timedelta
from itertools import pairwise
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

SOURCE = "order_events"
DEFAULT_TOPIC = "orders"

# (event_type, the Olist column whose non-null value triggers it).
# Order matters: it is the canonical lifecycle order, used to check that a
# row's timestamps actually ascend.
LIFECYCLE: tuple[tuple[str, str], ...] = (
    ("created", "order_purchase_timestamp"),
    ("approved", "order_approved_at"),
    ("shipped", "order_delivered_carrier_date"),
    ("delivered", "order_delivered_customer_date"),
)

# Final statuses with no timestamp column of their own. These get one
# synthesized event, explicitly flagged as such in the payload.
TERMINAL_STATUSES = frozenset({"canceled", "unavailable"})

AVRO_SCHEMA: dict[str, Any] = {
    "type": "record",
    "name": "OrderEvent",
    "namespace": "ledgerline.orders",
    "doc": "One status transition of one order. Append-only; never updated in place.",
    "fields": [
        {"name": "event_id", "type": "string", "doc": "sha256 of the natural key; stable across replays"},
        {
            "name": "event_ts",
            "type": {"type": "long", "logicalType": "timestamp-micros"},
            "doc": "business time",
        },
        {
            "name": "produced_at",
            "type": {"type": "long", "logicalType": "timestamp-micros"},
            "doc": "processing time",
        },
        {"name": "source", "type": "string"},
        {"name": "schema_version", "type": "int"},
        # string, not enum: adding a status later stays a backward-compatible
        # change without touching Schema Registry compatibility rules.
        {"name": "event_type", "type": "string"},
        {"name": "order_id", "type": "string"},
        {
            "name": "customer_id",
            "type": "string",
            "doc": "PER-ORDER id, not the person. Join to customers for customer_unique_id.",
        },
        {"name": "order_status", "type": "string", "doc": "the order's final status, carried on every event"},
        {"name": "ts_is_synthetic", "type": "boolean", "default": False},
        {
            "name": "estimated_delivery_date",
            "type": ["null", {"type": "long", "logicalType": "timestamp-micros"}],
            "default": None,
        },
        {
            "name": "items",
            "doc": "present on 'created' only; one entry per unit ordered",
            "type": [
                "null",
                {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "OrderItem",
                        "fields": [
                            {"name": "order_item_id", "type": "int"},
                            {"name": "product_id", "type": "string"},
                            {"name": "seller_id", "type": "string"},
                            {"name": "price", "type": "double"},
                            {"name": "freight_value", "type": "double"},
                        ],
                    },
                },
            ],
            "default": None,
        },
    ],
}


def _items_by_order(items: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    """Group order_items into per-order lists, ordered by order_item_id.

    Olist stores one row per *unit*, so a 3-unit order has order_item_id 1,2,3.
    Preserving that shape matters: the reconciliation test counts units, and
    collapsing to distinct products here would quietly change the grain.
    """
    grouped: dict[str, list[dict[str, Any]]] = {}
    ordered = items.sort_values(["order_id", "order_item_id"], kind="stable")
    for row in ordered.itertuples(index=False):
        grouped.setdefault(row.order_id, []).append(
            {
                "order_item_id": int(row.order_item_id),
                "product_id": row.product_id,
                "seller_id": row.seller_id,
                "price": float(row.price) if pd.notna(row.price) else 0.0,
                "freight_value": float(row.freight_value) if pd.notna(row.freight_value) else 0.0,
            }
        )
    return grouped


def build_events(
    orders: pd.DataFrame,
    items: pd.DataFrame,
    produced_at=None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Fan orders out into events, globally sorted by event time.

    Returns ``(events, stats)``. Sorting globally by ``event_ts`` is what makes
    this a realistic stream: events from different orders interleave, so a
    consumer genuinely sees an ``approved`` for one order between two other
    orders' ``created``.

    The secondary sort key is the lifecycle rank, so that if two of an order's
    own timestamps collide to the microsecond, ``created`` still precedes
    ``approved`` rather than landing in arbitrary order.
    """
    rank = {name: i for i, (name, _) in enumerate(LIFECYCLE)}
    per_order_items = _items_by_order(items)
    events: list[dict[str, Any]] = []
    stats = {
        "orders": len(orders),
        "synthetic_terminal": 0,
        "non_monotonic_orders": 0,
        "orders_without_items": 0,
    }

    for row in orders.itertuples(index=False):
        order_id = row.order_id
        status = row.order_status if pd.notna(row.order_status) else "unknown"
        estimated = row.order_estimated_delivery_date
        seen: list[pd.Timestamp] = []
        last_ts: pd.Timestamp | None = None

        for event_type, column in LIFECYCLE:
            value = getattr(row, column)
            if pd.isna(value):
                continue
            seen.append(value)
            last_ts = value if last_ts is None or value > last_ts else last_ts

            item_list = None
            if event_type == "created":
                item_list = per_order_items.get(order_id)
                if item_list is None:
                    stats["orders_without_items"] += 1

            events.append(
                _make_event(
                    order_id=order_id,
                    customer_id=row.customer_id,
                    event_type=event_type,
                    event_ts=value,
                    status=status,
                    estimated=estimated,
                    items=item_list,
                    synthetic=False,
                    produced_at=produced_at,
                    rank=rank[event_type],
                )
            )

        # Olist's own timestamps are not always monotonic (a handful of rows
        # are approved before they are purchased). Count them rather than
        # silently reordering — it is a real data-quality fact about the source.
        if len(seen) > 1 and any(b < a for a, b in pairwise(seen)):
            stats["non_monotonic_orders"] += 1

        if status in TERMINAL_STATUSES and last_ts is not None:
            # No column carries a cancellation time, so place it one second
            # after the last real event and flag it. Inventing the timestamp is
            # unavoidable; hiding that we invented it would not be.
            events.append(
                _make_event(
                    order_id=order_id,
                    customer_id=row.customer_id,
                    event_type=status,
                    event_ts=last_ts + timedelta(seconds=1),
                    status=status,
                    estimated=estimated,
                    items=None,
                    synthetic=True,
                    produced_at=produced_at,
                    rank=len(LIFECYCLE),
                )
            )
            stats["synthetic_terminal"] += 1

    events.sort(key=lambda e: (e["event_ts"], e["_rank"], e["order_id"]))
    for event in events:
        del event["_rank"]

    stats["events"] = len(events)
    return events, stats


def _make_event(
    *,
    order_id: str,
    customer_id: str,
    event_type: str,
    event_ts,
    status: str,
    estimated,
    items,
    synthetic: bool,
    produced_at,
    rank: int,
) -> dict[str, Any]:
    timestamp = event_ts.to_pydatetime() if hasattr(event_ts, "to_pydatetime") else event_ts
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=ASSUMED_TZ)

    # The natural key is (order, transition). Two runs of this generator over
    # the same CSV produce the same 32 hex chars, which is what lets the
    # exactly-once experiment assert on content rather than only on offsets.
    event = envelope(
        deterministic_id(SOURCE, order_id, event_type, timestamp.isoformat()),
        timestamp,
        SOURCE,
        produced_at,
    )
    estimated_micros = None
    if estimated is not None and pd.notna(estimated):
        est = estimated.to_pydatetime() if hasattr(estimated, "to_pydatetime") else estimated
        if est.tzinfo is None:
            est = est.replace(tzinfo=ASSUMED_TZ)
        estimated_micros = int(est.timestamp() * 1_000_000)

    event.update(
        {
            "event_type": event_type,
            "order_id": order_id,
            "customer_id": customer_id,
            "order_status": status,
            "ts_is_synthetic": synthetic,
            "estimated_delivery_date": estimated_micros,
            "items": items,
            "_rank": rank,
        }
    )
    return event


def emit(
    events: list[dict[str, Any]],
    sink: MessageSink,
    topic: str = DEFAULT_TOPIC,
    clock: ReplayClock | None = None,
) -> int:
    """Send events to the sink, keyed by ``order_id``.

    The key is the decision that matters here. Kafka guarantees ordering only
    *within a partition*, and the partition is chosen by key hash. Keying on
    ``order_id`` puts an order's whole lifecycle on one partition, so a
    consumer can never see ``delivered`` before ``created`` for that order.
    Keying on ``event_id`` (or round-robin) would spread them across
    partitions and lose that guarantee for no gain.
    """
    from generators._common import from_micros

    for event in events:
        if clock is not None and clock.paced:
            clock.sleep_until(from_micros(event["event_ts"]))
        sink.send(topic, event["order_id"], event)
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
    parser.add_argument(
        "--speedup",
        type=float,
        default=0.0,
        help="business seconds per wall second; 0 = unpaced (default). 2592000 = 1 month/sec",
    )
    parser.add_argument("--print-schema", action="store_true", help="dump the Avro schema and exit")
    args = parser.parse_args()

    if args.print_schema:
        print(json.dumps(AVRO_SCHEMA, indent=2))
        return 0

    orders = olist.load("orders", args.raw_dir).sort_values("order_purchase_timestamp", kind="stable")
    if args.limit:
        orders = orders.head(args.limit)
    items = olist.load("order_items", args.raw_dir)
    items = items[items["order_id"].isin(set(orders["order_id"]))]

    events, stats = build_events(orders, items)

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
        sent = emit(events, sink, args.topic, clock)
    finally:
        sink.close()

    print(f"  orders read              {stats['orders']:>9,}")
    print(f"  events emitted           {sent:>9,}  -> topic {args.topic!r}")
    print(f"  synthetic terminal evts  {stats['synthetic_terminal']:>9,}  (canceled / unavailable)")
    print(f"  orders with no items     {stats['orders_without_items']:>9,}")
    print(f"  non-monotonic lifecycles {stats['non_monotonic_orders']:>9,}  (source data quality)")
    if args.sink == "jsonl":
        print(f"  written to               {args.out_dir / (args.topic + '.jsonl')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
