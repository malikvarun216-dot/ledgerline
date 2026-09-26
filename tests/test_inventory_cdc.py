"""Tests for the inventory CDC generator.

The assertions that matter are the three invariants in the module docstring:
negative delta implies a sale, stock never clamps, and ``seq`` is monotonic and
restart-safe. Everything Session 9 and Session 10 do rests on those.
"""

from __future__ import annotations

import pandas as pd

from generators._common import MemorySink, from_micros
from generators.inventory_cdc import (
    DEFAULT_TOPIC,
    OP_DELETE,
    OP_INSERT,
    OP_UPDATE,
    REASON_RESTOCK,
    REASON_SALE,
    assign_seq,
    build_cdc_events,
    emit,
    shuffle_out_of_order,
    sku_key,
    units_sold_from_cdc,
)

# (PROD_A,S1) 5 units / 3 orders · (PROD_B,S1) 2 / 2 · (PROD_NOCAT,S2) 1 / 1 · (PROD_C,S2) 1 / 1
EXPECTED_SKUS = 4
EXPECTED_UNITS = 9
EXPECTED_SALE_EVENTS = 7


def test_one_seed_insert_per_sku(order_items, orders):
    events, stats = build_cdc_events(order_items, orders, restock_every=0)

    seeds = [e for e in events if e["op"] == OP_INSERT]
    assert stats["skus"] == EXPECTED_SKUS
    assert len(seeds) == EXPECTED_SKUS
    assert {e["sku_key"] for e in seeds} == {
        sku_key("PROD_A", "SELLER_1"),
        sku_key("PROD_B", "SELLER_1"),
        sku_key("PROD_NOCAT", "SELLER_2"),
        sku_key("PROD_C", "SELLER_2"),
    }
    assert all(e["prev_stock_qty"] is None for e in seeds), "a seed has no before-image"


def test_seed_precedes_every_sale_for_its_sku(order_items, orders):
    """A SKU cannot be sold before it exists."""
    events, _ = build_cdc_events(order_items, orders, restock_every=0)

    first_seq: dict[str, int] = {}
    for event in sorted(events, key=lambda e: e["seq"]):
        if event["op"] == OP_INSERT:
            first_seq[event["sku_key"]] = event["seq"]
        else:
            assert event["sku_key"] in first_seq, f"{event['sku_key']} changed before its seed"
            assert event["seq"] > first_seq[event["sku_key"]]


def test_sales_are_grouped_per_order_not_per_unit(order_items, orders):
    """A three-unit line is one stock movement of -3, as a real system writes it."""
    events, stats = build_cdc_events(order_items, orders, restock_every=0)

    sales = [e for e in events if e["change_reason"] == REASON_SALE]
    assert len(sales) == EXPECTED_SALE_EVENTS
    assert stats["units_sold"] == EXPECTED_UNITS

    multi = [e for e in sales if e["sku_key"] == sku_key("PROD_A", "SELLER_1")]
    deltas = sorted(e["stock_qty"] - e["prev_stock_qty"] for e in multi)
    assert deltas == [-3, -1, -1]


# --------------------------------------------------------------------------
# Invariant 1 — negative delta implies a sale
# --------------------------------------------------------------------------


def test_units_sold_rebuilds_from_deltas_alone(order_items, orders):
    """The cross-source reconciliation, proved on the generator's own output.

    ``units_sold_from_cdc`` reads only before/after images — no change_reason,
    no join back to orders — so agreement here is a genuine second derivation
    of the same number, not a restatement of it.
    """
    events, stats = build_cdc_events(order_items, orders, restock_every=3, restock_qty=25)

    rebuilt = units_sold_from_cdc(events)
    assert sum(rebuilt.values()) == EXPECTED_UNITS == stats["units_sold"]
    assert rebuilt[sku_key("PROD_A", "SELLER_1")] == 5
    assert rebuilt[sku_key("PROD_B", "SELLER_1")] == 2


def test_restocks_are_strictly_positive(order_items, orders):
    """If a restock could be negative, 'negative delta = sale' would be false."""
    events, stats = build_cdc_events(order_items, orders, restock_every=3, restock_qty=25)

    restocks = [e for e in events if e["change_reason"] == REASON_RESTOCK]
    assert stats["restocks"] == 1, "only PROD_A|SELLER_1 reaches 3 sales"
    assert all(e["stock_qty"] - e["prev_stock_qty"] > 0 for e in restocks)

    negative = [e for e in events if e["prev_stock_qty"] is not None and e["stock_qty"] < e["prev_stock_qty"]]
    assert {e["change_reason"] for e in negative} == {REASON_SALE}


def test_restocks_do_not_disturb_the_tie_out(order_items, orders):
    """Units sold must be identical with restocks on and off."""
    with_restock, _ = build_cdc_events(order_items, orders, restock_every=1, restock_qty=100)
    without, _ = build_cdc_events(order_items, orders, restock_every=0)

    assert units_sold_from_cdc(with_restock) == units_sold_from_cdc(without)


# --------------------------------------------------------------------------
# Invariant 2 — stock never clamps
# --------------------------------------------------------------------------


def test_stock_never_goes_negative_or_clamps(order_items, orders):
    """A clamp would swallow a decrement and break the tie-out for the wrong reason."""
    events, _ = build_cdc_events(order_items, orders, restock_every=0, headroom=1, buffer_stock=1)

    assert all(e["stock_qty"] >= 0 for e in events)
    for event in events:
        if event["prev_stock_qty"] is not None and event["op"] != OP_DELETE:
            assert event["stock_qty"] != event["prev_stock_qty"], "a clamp shows up as a no-op delta"


def test_seed_stock_covers_lifetime_demand(order_items, orders):
    events, _ = build_cdc_events(order_items, orders, restock_every=0, headroom=2, buffer_stock=10)
    seeds = {e["sku_key"]: e["stock_qty"] for e in events if e["op"] == OP_INSERT}
    sold = units_sold_from_cdc(events)

    for key, units in sold.items():
        assert seeds[key] >= units, f"{key} seeded below lifetime demand"


# --------------------------------------------------------------------------
# Invariant 3 — seq monotonic, unique, restart-safe
# --------------------------------------------------------------------------


def test_seq_is_globally_unique_and_monotonic_in_event_time(order_items, orders):
    events, _ = build_cdc_events(order_items, orders, restock_every=3)

    by_seq = sorted(events, key=lambda e: e["seq"])
    assert len({e["seq"] for e in events}) == len(events), "seq collision"
    assert [e["event_ts"] for e in by_seq] == sorted(e["event_ts"] for e in events)


def test_seq_is_derived_from_event_time_not_counted(order_items, orders):
    """Derived, so a generator restart cannot reset it — a named danger zone."""
    events, _ = build_cdc_events(order_items, orders, restock_every=3)

    for event in events:
        assert event["seq"] // 1000 == event["event_ts"]
        assert event["seq"] % 1000 < 1000


def test_restarting_the_generator_reproduces_identical_seq(order_items, orders):
    first, _ = build_cdc_events(order_items, orders, restock_every=3)
    second, _ = build_cdc_events(order_items, orders, restock_every=3)

    assert [(e["sku_key"], e["seq"]) for e in first] == [(e["sku_key"], e["seq"]) for e in second]
    assert [e["event_id"] for e in first] == [e["event_id"] for e in second]


def test_assign_seq_rejects_a_microsecond_it_cannot_tiebreak():
    """Fail loudly rather than emit a duplicate seq."""
    import pytest

    crowded = [
        {"event_ts": 1_000_000, "sku_key": f"sku{i}", "change_reason": "sale", "seq": 0}
        for i in range(1001)
    ]
    with pytest.raises(RuntimeError, match="widen the seq tiebreaker"):
        assign_seq(crowded)


# --------------------------------------------------------------------------
# Out-of-order emission — Session 10 raw material
# --------------------------------------------------------------------------


def test_out_of_order_disturbs_arrival_but_not_seq(order_items, orders):
    """The feed keeps correct ordering information even when it arrives wrong.

    That separation is the whole experiment: a MERGE with ``s.seq > t.seq``
    survives this input, one without it clobbers newer state with older.
    """
    events, _ = build_cdc_events(order_items, orders, restock_every=3)
    shuffled = shuffle_out_of_order(events, percent=50, seed=1)

    assert len(shuffled) == len(events)
    assert [e["seq"] for e in shuffled] != [e["seq"] for e in events], "nothing was reordered"
    assert sorted(e["seq"] for e in shuffled) == sorted(e["seq"] for e in events)
    # sorting by seq restores the true order, which is what makes the guard work
    assert [e["event_id"] for e in sorted(shuffled, key=lambda e: e["seq"])] == [
        e["event_id"] for e in events
    ]


def test_zero_percent_leaves_order_untouched(order_items, orders):
    events, _ = build_cdc_events(order_items, orders)
    assert [e["seq"] for e in shuffle_out_of_order(events, 0)] == [e["seq"] for e in events]


# --------------------------------------------------------------------------
# Deletes and keying
# --------------------------------------------------------------------------


def test_delist_lands_after_every_sale_for_that_sku(order_items, orders):
    """A delete before a decrement would orphan the decrement."""
    events, stats = build_cdc_events(order_items, orders, restock_every=0, delist_count=2, seed=1)

    deletes = [e for e in events if e["op"] == OP_DELETE]
    assert stats["delists"] == 2 == len(deletes)

    for delete in deletes:
        same_sku = [e for e in events if e["sku_key"] == delete["sku_key"] and e["op"] != OP_DELETE]
        assert all(e["seq"] < delete["seq"] for e in same_sku)
    assert all(e["prev_stock_qty"] == e["stock_qty"] for e in deletes), "a delist moves no stock"


def test_delete_does_not_count_as_a_sale(order_items, orders):
    with_delists, _ = build_cdc_events(order_items, orders, restock_every=0, delist_count=3, seed=1)
    without, _ = build_cdc_events(order_items, orders, restock_every=0, delist_count=0)
    assert units_sold_from_cdc(with_delists) == units_sold_from_cdc(without)


def test_messages_are_keyed_by_sku(order_items, orders):
    events, _ = build_cdc_events(order_items, orders, restock_every=3)
    sink = MemorySink()

    assert emit(events, sink, DEFAULT_TOPIC) == len(events)
    assert sink.keys_for_topic(DEFAULT_TOPIC) == [e["sku_key"] for e in events]
    assert len(set(sink.keys_for_topic(DEFAULT_TOPIC))) == EXPECTED_SKUS


def test_ops_are_only_the_three_debezium_flags(order_items, orders):
    events, _ = build_cdc_events(order_items, orders, restock_every=3, delist_count=1, seed=1)
    assert {e["op"] for e in events} == {OP_INSERT, OP_UPDATE, OP_DELETE}


def test_seed_timestamp_precedes_the_skus_own_first_sale(order_items, orders):
    """Each SKU seeds before ITS first sale, not before the dataset's first order.

    PROD_A|SELLER_1 first sells on 2017-01-10 (ORD_MULTI); PROD_C|SELLER_2 first
    sells on 2017-04-07 (ORD_PARTIAL) — nearly three months later. A single
    shared seed timestamp for every SKU would put PROD_C's seed absurdly early,
    and at real Olist scale (34k distinct SKUs) it collides thousands of events
    onto one microsecond. See docs/incidents.md [2026-09-19].
    """
    events, _ = build_cdc_events(order_items, orders, restock_every=0)
    seeds = {e["sku_key"]: from_micros(e["event_ts"]) for e in events if e["op"] == OP_INSERT}

    first_sale_by_sku: dict[str, pd.Timestamp] = {}
    joined = order_items.merge(orders[["order_id", "order_purchase_timestamp"]], on="order_id")
    for row in joined.itertuples():
        key = sku_key(row.product_id, row.seller_id)
        ts = pd.Timestamp(row.order_purchase_timestamp).tz_localize("UTC")
        first_sale_by_sku[key] = min(first_sale_by_sku.get(key, ts), ts)

    assert len(seeds) > 1
    for key, seed_time in seeds.items():
        assert seed_time < first_sale_by_sku[key], f"{key} seeded after its own first sale"

    # and it is NOT one shared instant across every SKU
    assert len(set(seeds.values())) > 1, "every SKU seeded at the same timestamp — the bug is back"


def test_many_skus_do_not_collide_on_one_seed_timestamp():
    """Regression for the 34k-SKU seed collision found against the real dataset.

    With a single shared seed instant, this many SKUs blows through
    assign_seq's 1000-per-microsecond tiebreaker and raises. Per-SKU seeding
    spreads them out because each SKU's first sale is a distinct minute.
    """
    n = 1200
    order_ids = [f"ORD_{i}" for i in range(n)]
    base = pd.Timestamp("2017-01-01 00:00:00")

    orders = pd.DataFrame(
        {
            "order_id": order_ids,
            "customer_id": [f"cid_{i}" for i in range(n)],
            "order_status": "delivered",
            "order_purchase_timestamp": [base + pd.Timedelta(minutes=i) for i in range(n)],
            "order_approved_at": pd.NaT,
            "order_delivered_carrier_date": pd.NaT,
            "order_delivered_customer_date": pd.NaT,
            "order_estimated_delivery_date": pd.NaT,
        }
    )
    order_items = pd.DataFrame(
        {
            "order_id": order_ids,
            "order_item_id": 1,
            "product_id": [f"PROD_{i}" for i in range(n)],
            "seller_id": "SELLER_1",
            "shipping_limit_date": base,
            "price": 10.0,
            "freight_value": 1.0,
        }
    )

    events, stats = build_cdc_events(order_items, orders, restock_every=0)
    assert stats["skus"] == n
    assert len({e["seq"] for e in events}) == len(events), "seq collision"


# --------------------------------------------------------------------------
# Partial runs must not reach the topic  (Session 5)
# --------------------------------------------------------------------------


def _run_cdc(argv: list[str]) -> tuple[int, str]:
    """Run the generator's ``main`` with ``argv``, returning (exit code, stderr)."""
    import contextlib
    import io
    import sys

    from generators import inventory_cdc

    stderr = io.StringIO()
    old = sys.argv
    sys.argv = ["inventory_cdc.py", *argv]
    try:
        with contextlib.redirect_stderr(stderr), contextlib.redirect_stdout(io.StringIO()):
            code = inventory_cdc.main()
    except SystemExit as exit_signal:
        code = exit_signal.code
        if isinstance(code, str):
            stderr.write(code)
            code = 1
    finally:
        sys.argv = old
    return code, stderr.getvalue()


def test_partial_cdc_run_is_refused_for_kafka():
    """A --limit CDC run is a different dataset, not a smaller one.

    Seed stock is ``headroom x lifetime demand`` measured over the orders the
    run was given, so the same SKU seeded from 50 orders and from 99,441 orders
    carries a different ``stock_qty`` — while sharing a ``sku_key`` and a
    ``seq``, because ``seq`` comes from event time and event time does not
    depend on ``--limit``.

    Session 5 measured 53 such pairs sitting on the live topic, left there by
    Session 4's smoke run: 43 SKUs whose ``seq`` values are tied rather than
    ascending. Silver's ``s.seq > t.seq`` is strictly greater, so it keeps
    whichever landed first and discards the other without raising — a wrong
    stock level that persists instead of an error that surfaces.

    The guard must fire *before* a producer is constructed, so this passes no
    credentials and still expects a clean refusal rather than a connection
    error.
    """
    code, message = _run_cdc(["--sink", "kafka", "--limit", "50"])

    assert code == 1
    assert "refusing" in message
    assert "--limit 50" in message
    assert "seq" in message, "the error must name the mechanism, not just refuse"
    assert "--allow-partial" in message, "the error must name the escape hatch"


def test_partial_cdc_run_is_allowed_when_asked_for_explicitly():
    """Session 10 contaminates the topic on purpose, so the door must open.

    Checked by confirming the guard is not what stops it: with the flag set the
    run gets past the refusal and fails on the missing broker instead, which is
    a different failure entirely.
    """
    code, message = _run_cdc(["--sink", "kafka", "--limit", "50", "--allow-partial"])

    assert "refusing" not in message
    assert code != 0 or message == "", "no broker in tests, so this cannot succeed"


def test_order_events_has_no_partial_guard_and_that_is_deliberate():
    """Orders genuinely are a prefix, so guarding them would be cargo-culted.

    An order's lifecycle is built from its own timestamps and does not depend
    on how many other orders were loaded. Measured rather than assumed: all 203
    records Session 4's 50-order run left on the `orders` topic turned out to
    be byte-identical duplicates of full-run events, which is why the full
    readback reports 394,293 records against 394,090 distinct ids.

    This asserts the asymmetry is real, so that "add the same guard to both for
    consistency" has to argue with a test rather than look tidy.
    """
    import inspect

    from generators import inventory_cdc, order_events

    assert "allow_partial" in inspect.getsource(inventory_cdc.main)
    assert "allow_partial" not in inspect.getsource(order_events.main)
