"""Tests for the order event generator.

The assertions here are about *stream shape*, not formatting: fan-out counts,
global event-time ordering, replay determinism, and the partition key. Each of
those is something a downstream Bronze/Silver assumption rests on.
"""

from __future__ import annotations

import pandas as pd

from generators import olist
from generators._common import MemorySink, deterministic_id, from_micros
from generators.order_events import (
    DEFAULT_TOPIC,
    LIFECYCLE,
    build_events,
    emit,
)

# ORD_MULTI 4 + ORD_OVERLAP 4 + ORD_SIMPLE 4 + ORD_CANCEL 2+1
# + ORD_PARTIAL 3 + ORD_REPEAT 4 + ORD_OPEN 1
EXPECTED_EVENTS = 23


def test_fans_each_order_out_to_one_event_per_real_timestamp(orders, order_items):
    events, stats = build_events(orders, order_items)

    assert stats["orders"] == 7
    assert len(events) == EXPECTED_EVENTS

    by_order: dict[str, list[str]] = {}
    for event in events:
        by_order.setdefault(event["order_id"], []).append(event["event_type"])

    assert by_order["ORD_MULTI"] == ["created", "approved", "shipped", "delivered"]
    # approved is null in the middle of the lifecycle; the gap is skipped, not filled
    assert by_order["ORD_PARTIAL"] == ["created", "shipped", "delivered"]
    # only ever purchased
    assert by_order["ORD_OPEN"] == ["created"]


def test_canceled_order_gets_one_flagged_synthetic_event(orders, order_items):
    events, stats = build_events(orders, order_items)
    cancel_events = [e for e in events if e["order_id"] == "ORD_CANCEL"]

    assert [e["event_type"] for e in cancel_events] == ["created", "approved", "canceled"]
    assert stats["synthetic_terminal"] == 1

    synthetic = cancel_events[-1]
    assert synthetic["ts_is_synthetic"] is True
    # the two real events must NOT claim a synthetic timestamp
    assert all(e["ts_is_synthetic"] is False for e in cancel_events[:-1])

    # placed one second after the last real timestamp (approved at 12:05:00)
    assert from_micros(synthetic["event_ts"]).isoformat() == "2017-03-01T12:05:01+00:00"


def test_events_are_globally_sorted_by_event_time(orders, order_items):
    """Different orders must interleave, or this is not a stream.

    A per-order-then-concatenate ordering would still be sorted *within* an
    order while presenting an unrealistic feed to the consumer.
    """
    events, _ = build_events(orders, order_items)

    timestamps = [e["event_ts"] for e in events]
    assert timestamps == sorted(timestamps)

    order_sequence = [e["order_id"] for e in events]
    first_positions = {oid: order_sequence.index(oid) for oid in set(order_sequence)}
    last_positions = {
        oid: len(order_sequence) - 1 - order_sequence[::-1].index(oid) for oid in first_positions
    }
    interleaved = any(
        first_positions[a] < first_positions[b] < last_positions[a]
        for a in first_positions
        for b in first_positions
        if a != b
    )
    assert interleaved, "no two orders overlap in the stream — events are not interleaved"


def test_lifecycle_rank_breaks_ties_at_identical_timestamps(order_items):
    """Two transitions at the same instant must still come out in lifecycle order."""
    same = pd.Timestamp("2017-01-05 10:00:00")
    orders = pd.DataFrame(
        [("ORD_TIE", "cid_1", "delivered", same, same, same, same, pd.NaT)],
        columns=[
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    )
    events, _ = build_events(orders, order_items)
    assert [e["event_type"] for e in events] == [name for name, _ in LIFECYCLE]


def test_replay_produces_byte_identical_event_ids(orders, order_items):
    """Idempotency depends on this.

    If a second run produced different IDs, a dedup-on-event_id at Silver could
    never be shown to work — only Kafka's offset mechanism would be under test.
    """
    first, _ = build_events(orders, order_items)
    second, _ = build_events(orders, order_items)

    assert [e["event_id"] for e in first] == [e["event_id"] for e in second]
    assert len({e["event_id"] for e in first}) == len(first), "event_id is not unique per event"


def test_produced_at_is_independent_of_event_ts(orders, order_items):
    """Business time and processing time must be separately observable."""
    wall = pd.Timestamp("2026-09-19 08:00:00", tz="UTC").to_pydatetime()
    events, _ = build_events(orders, order_items, produced_at=wall)

    assert len({e["produced_at"] for e in events}) == 1, "produced_at should be the emit instant"
    assert len({e["event_ts"] for e in events}) > 1, "event_ts should vary with business time"
    assert all(e["produced_at"] > e["event_ts"] for e in events), "2026 replay of 2017 data"


def test_items_ride_only_on_created_and_keep_unit_grain(orders, order_items):
    events, stats = build_events(orders, order_items)

    created = {e["order_id"]: e for e in events if e["event_type"] == "created"}
    # one row per unit: three units of the same product, not one collapsed row
    assert len(created["ORD_MULTI"]["items"]) == 3
    assert {i["product_id"] for i in created["ORD_MULTI"]["items"]} == {"PROD_A"}
    assert [i["order_item_id"] for i in created["ORD_MULTI"]["items"]] == [1, 2, 3]

    assert created["ORD_OPEN"]["items"] is None
    assert stats["orders_without_items"] == 1

    assert all(e["items"] is None for e in events if e["event_type"] != "created")


def test_messages_are_keyed_by_order_id(orders, order_items):
    """Kafka orders within a partition only; the key picks the partition."""
    events, _ = build_events(orders, order_items)
    sink = MemorySink()

    assert emit(events, sink, DEFAULT_TOPIC) == EXPECTED_EVENTS
    assert sink.keys_for_topic(DEFAULT_TOPIC) == [e["order_id"] for e in events]
    assert set(sink.keys_for_topic(DEFAULT_TOPIC)) == set(orders["order_id"])


def test_non_monotonic_source_timestamps_are_counted_not_reordered(order_items):
    """Olist approves a few orders before they are purchased. Report, do not hide."""
    orders = pd.DataFrame(
        [(
            "ORD_BACKWARDS", "cid_1", "delivered",
            pd.Timestamp("2017-01-05 10:00:00"),
            pd.Timestamp("2017-01-04 09:00:00"),  # approved BEFORE purchase
            pd.NaT, pd.NaT, pd.NaT,
        )],
        columns=[
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    )
    events, stats = build_events(orders, order_items)

    assert stats["non_monotonic_orders"] == 1
    # the stream still sorts by event time, so 'approved' genuinely comes first
    assert [e["event_type"] for e in events] == ["approved", "created"]


def test_generator_reads_through_the_real_csv_loader(raw_dir):
    """Exercise the data/raw path, not just in-memory frames."""
    orders = olist.load("orders", raw_dir)
    items = olist.load("order_items", raw_dir)
    events, stats = build_events(orders, items)

    assert stats["orders"] == 7
    assert len(events) == EXPECTED_EVENTS
    assert all(isinstance(e["event_ts"], int) for e in events)


def test_deterministic_id_separator_prevents_boundary_collisions():
    """('ab','c') and ('a','bc') must not hash alike — a silent event merge."""
    assert deterministic_id("ab", "c") != deterministic_id("a", "bc")
    # absent and blank are different states and must not merge either
    assert deterministic_id("x", None) != deterministic_id("x", "")
