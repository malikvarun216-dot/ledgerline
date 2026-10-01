"""The project's headline numbers, re-derived by running the generators on the real dataset.

Session 5 found the CDC event count written as 158,396 in three sessions of docs; the generator
had produced 158,346 all along (incidents.md, 2026-09-20). No test asserted the total, so a number
that lived only in prose could never be contradicted. Drill 1 built this guard: every number the
docs, the Bronze notebooks and the reconciliation lean on is asserted here, from the generators.

Needs the real Olist CSVs in data/raw/ (about 22 s). CI does not have them — they come from
Kaggle under the dataset's licence — so there these tests skip, and `pytest -ra` lists the skip.
The same totals are also asserted in Databricks against the topics themselves (stream_orders,
stream_cdc), so the numbers are checked on both sides of Kafka.
"""

from __future__ import annotations

from collections import Counter
from functools import cache
from pathlib import Path

import pytest

from generators import inventory_cdc, olist, order_events

HAS_OLIST = all((olist.DEFAULT_RAW_DIR / t.filename).exists() for t in olist.TABLES.values())
needs_olist = pytest.mark.skipif(not HAS_OLIST, reason="real Olist CSVs not in data/raw (not in CI)")


@cache
def _full_run():
    raw = Path(olist.DEFAULT_RAW_DIR)
    orders = olist.load("orders", raw).sort_values("order_purchase_timestamp", kind="stable")
    items = olist.load("order_items", raw)
    events, stats = order_events.build_events(orders, items)
    cdc, cdc_stats = inventory_cdc.build_cdc_events(
        items, orders, headroom=2, restock_every=3, restock_qty=25, delist_count=0, seed=20260919
    )
    return events, stats, cdc, cdc_stats


@needs_olist
def test_order_stream_is_394090_events_from_99441_orders():
    events, stats, _, _ = _full_run()
    assert stats["orders"] == 99_441
    assert len(events) == 394_090
    assert len({e["event_id"] for e in events}) == 394_090


@needs_olist
def test_cdc_stream_is_158346_events_and_decomposes_exactly():
    """158,346, not the 158,396 three sessions of docs carried."""
    _, _, cdc, s = _full_run()
    assert len(cdc) == 158_346
    shape = (s["skus"], s["seeds"], s["sales"], s["restocks"], s["delists"])
    assert shape == (34_448, 34_448, 102_425, 21_473, 0)
    assert s["seeds"] + s["sales"] + s["restocks"] + s["delists"] == len(cdc)


@needs_olist
def test_units_sold_ties_out_across_the_two_streams():
    """The cross-source reconciliation: 112,650 units on orders == 112,650 decremented in CDC."""
    events, _, cdc, _ = _full_run()
    # An order with no line items carries items=None: 0 units, as Bronze's sum(size(items)) counts it.
    ordered = sum(len(e["items"] or []) for e in events if e["event_type"] == "created")
    assert ordered == 112_650
    assert sum(inventory_cdc.units_sold_from_cdc(cdc).values()) == 112_650


# Silver orders: one row per order, one time per lifecycle step (databricks/silver/_merge_orders).
# The status precedence is the notebook's STATUS_PRECEDENCE; the numbers below are the ones
# merge_orders asserts against silver.orders.
STATUS_PRECEDENCE = ("canceled", "unavailable", "delivered", "shipped", "approved", "created")


def _order_snapshot(events):
    """order_id -> {step: event_ts}, plus the events' own arrival order (the generator's sort)."""
    steps, arrival = {}, {}
    for event in events:
        steps.setdefault(event["order_id"], {})[event["event_type"]] = event["event_ts"]
        arrival.setdefault(event["order_id"], []).append(event["event_type"])
    return steps, arrival


def _status(times):
    return next(s for s in STATUS_PRECEDENCE if s in times)


@needs_olist
def test_silver_orders_answer_key():
    events, _, _, _ = _full_run()
    steps, arrival = _order_snapshot(events)
    assert len(steps) == 99_441
    statuses = Counter(_status(t) for t in steps.values())
    assert statuses == {
        "delivered": 96_470, "shipped": 1_114, "canceled": 625,
        "approved": 618, "unavailable": 609, "created": 5,
    }
    per_step = {s: [t[s] // 1_000_000 for t in steps.values() if s in t] for s in STATUS_PRECEDENCE}
    assert {s: (len(v), sum(v)) for s, v in per_step.items()} == {
        "created": (99_441, 150_624_256_503_496),
        "approved": (99_281, 150_385_430_521_853),
        "shipped": (97_658, 147_961_888_552_408),
        "delivered": (96_476, 146_251_032_384_713),
        "canceled": (625, 945_845_962_563),
        "unavailable": (609, 917_417_673_520),
    }
    # Why one column per step (decisions.md, Session 10): "the last event to arrive wins" gives a
    # different status on 93 orders, because Olist's own timestamps run backwards on some of them.
    last_wins = Counter(
        (arrival[o][-1], _status(t)) for o, t in steps.items() if arrival[o][-1] != _status(t)
    )
    assert last_wins == {
        ("approved", "delivered"): 61, ("shipped", "delivered"): 23, ("approved", "shipped"): 9,
    }
    # 166 orders arrive with `shipped` first, so "created_at is set" can never be a constraint.
    assert Counter(a[0] for a in arrival.values() if a[0] != "created") == {"shipped": 166}
    # And 1,305 orders have two steps at one timestamp: an event_ts "seq" guard would drop one.
    assert sum(len(set(t.values())) < len(t) for t in steps.values()) == 1_305


@needs_olist
def test_silver_orders_source_status_differs_where_no_event_can_say_it():
    """The status Olist printed on the row vs the furthest step reached: 623 orders differ, all of them
    statuses no event carries (`invoiced`, `processing`) or a `delivered` with no delivery time."""
    events, _, _, _ = _full_run()
    steps, _ = _order_snapshot(events)
    source = {e["order_id"]: e["order_status"] for e in events}
    differ = Counter((source[o], _status(t)) for o, t in steps.items() if source[o] != _status(t))
    assert differ == {
        ("invoiced", "approved"): 314, ("processing", "approved"): 301,
        ("delivered", "shipped"): 7, ("delivered", "approved"): 1,
    }


@needs_olist
def test_units_sold_tie_out_per_sku():
    """Silver's version of the reconciliation: per SKU, units on order items == units the CDC feed
    decremented. All 34,448 SKUs, not only the total — a total can hide two SKUs that are off by
    opposite amounts."""
    events, _, cdc, _ = _full_run()
    ordered = Counter(
        f"{i['product_id']}|{i['seller_id']}"
        for e in events if e["event_type"] == "created" for i in (e["items"] or [])
    )
    sold = inventory_cdc.units_sold_from_cdc(cdc)
    assert len(ordered) == len(sold) == 34_448
    assert ordered == Counter(sold)
