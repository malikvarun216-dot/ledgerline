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
