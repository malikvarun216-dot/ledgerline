"""Tests for the nightly dimension dumps.

Two properties carry almost all the weight here:

1. the customer dimension is keyed on the **person**, not the order, and
2. ``dim_updated_at`` moves only when a row's content actually changes.

Both are failure modes that produce a *working* pipeline with silently wrong
history, which is the worst kind — nothing errors, the SCD2 table is just junk.
"""

from __future__ import annotations

import io
from datetime import date, datetime

import numpy as np
import pandas as pd
import pytest

from generators import olist
from generators._common import LocalBlobSink
from generators.dim_dumps import (
    CUSTOMER,
    DUMP_DATE,
    PRODUCT,
    SELLER,
    UPDATED_AT,
    LandedNightChanged,
    apply_synthetic_changes,
    attribute_hash,
    customer_snapshot,
    customer_timeline,
    deleted_keys,
    dump_key,
    run,
    write_night,
)
from tests.conftest import CU_DOOMED, CU_MOVER, CU_SOLO, CU_STAYER

# Chosen so each night lands just after a real observation in the fixture.
START = date(2017, 1, 31)
STRIDE = 28


@pytest.fixture
def sink(tmp_path) -> LocalBlobSink:
    return LocalBlobSink(tmp_path / "dumps")


def read_dump(sink: LocalBlobSink, dimension: str, dump_date: date) -> pd.DataFrame:
    frame = pd.read_csv(io.StringIO(sink.get_text(dump_key(dimension, dump_date))))
    frame[UPDATED_AT] = pd.to_datetime(frame[UPDATED_AT])
    return frame


def nights(count: int) -> list[date]:
    from datetime import timedelta

    return [START + timedelta(days=i * STRIDE) for i in range(count)]


# --------------------------------------------------------------------------
# The customer_id / customer_unique_id trap
# --------------------------------------------------------------------------


def test_customer_dimension_is_keyed_on_the_person_not_the_order(customers, orders):
    """7 order-level customer_id rows collapse to 4 actual people."""
    timeline = customer_timeline(customers, orders)
    assert len(timeline) == 7, "one observation per order"

    snapshot = customer_snapshot(timeline, datetime(2017, 12, 31))
    assert CUSTOMER.key == "customer_unique_id"
    assert len(snapshot) == 4, "keyed on customer_id this would be 7"
    assert set(snapshot[CUSTOMER.key]) == {CU_MOVER, CU_STAYER, CU_SOLO, CU_DOOMED}
    assert "customer_id" not in snapshot.columns, "the per-order id must not leak into the dimension"


def test_snapshot_shows_the_person_as_they_were_at_that_date(customers, orders):
    timeline = customer_timeline(customers, orders)

    january = customer_snapshot(timeline, datetime(2017, 1, 31, 23, 59))
    february = customer_snapshot(timeline, datetime(2017, 2, 28, 23, 59))

    mover_jan = january.set_index(CUSTOMER.key).loc[CU_MOVER]
    mover_feb = february.set_index(CUSTOMER.key).loc[CU_MOVER]
    assert mover_jan["customer_city"] == "sao paulo"
    assert mover_feb["customer_city"] == "rio de janeiro"


def test_a_person_is_absent_until_their_first_order(customers, orders):
    """Not yet existing and having been deleted look identical in one file.

    Only the sequence of dumps tells them apart — which is why the dumps are
    full snapshots rather than deltas.
    """
    timeline = customer_timeline(customers, orders)
    early = customer_snapshot(timeline, datetime(2017, 1, 31))
    assert CU_DOOMED not in set(early[CUSTOMER.key])  # first orders 2017-06-11
    assert CU_STAYER not in set(early[CUSTOMER.key])  # first orders 2017-03-01

    late = customer_snapshot(timeline, datetime(2017, 12, 31))
    assert CU_DOOMED in set(late[CUSTOMER.key])


# --------------------------------------------------------------------------
# dim_updated_at: business time, carried forward
# --------------------------------------------------------------------------


def test_unchanged_row_keeps_last_nights_timestamp(olist_frames, sink):
    """The single most important assertion in this file.

    CU_STAYER orders again on 2017-04-02 from the *same* city. A newer
    observation exists, so a naive generator would stamp the row 2017-04-02 and
    dbt would open a second SCD2 version for a customer who did not change.
    """
    run(olist_frames, sink, start=START, nights=4, stride_days=STRIDE, change_rate=0)
    third, fourth = nights(4)[2], nights(4)[3]

    before = read_dump(sink, "customer", third).set_index(CUSTOMER.key)
    after = read_dump(sink, "customer", fourth).set_index(CUSTOMER.key)

    assert CU_STAYER in before.index and CU_STAYER in after.index
    assert before.loc[CU_STAYER, "customer_city"] == after.loc[CU_STAYER, "customer_city"]
    assert after.loc[CU_STAYER, UPDATED_AT] == before.loc[CU_STAYER, UPDATED_AT]
    assert after.loc[CU_STAYER, UPDATED_AT] == pd.Timestamp("2017-03-01 12:00:00")


def test_changed_row_takes_the_real_business_timestamp(olist_frames, sink):
    """A genuine move stamps the observation time, not the dump date."""
    run(olist_frames, sink, start=START, nights=2, stride_days=STRIDE, change_rate=0)
    first, second = nights(2)

    before = read_dump(sink, "customer", first).set_index(CUSTOMER.key)
    after = read_dump(sink, "customer", second).set_index(CUSTOMER.key)

    assert before.loc[CU_MOVER, "customer_city"] == "sao paulo"
    assert after.loc[CU_MOVER, "customer_city"] == "rio de janeiro"

    assert before.loc[CU_MOVER, UPDATED_AT] == pd.Timestamp("2017-01-05 10:00:00")
    # the real order time, NOT the 2017-02-28 dump date
    assert after.loc[CU_MOVER, UPDATED_AT] == pd.Timestamp("2017-02-10 09:00:00")
    assert after.loc[CU_MOVER, UPDATED_AT].date() != second


def test_dim_updated_at_is_not_the_run_clock(olist_frames, sink, tmp_path):
    """Regenerating the same nights later must produce identical timestamps.

    If ``dim_updated_at`` were stamped with ``now()``, these two runs would
    differ — and every row would look changed on every nightly dump.
    """
    run(olist_frames, sink, start=START, nights=3, stride_days=STRIDE, change_rate=0)
    first_run = [read_dump(sink, "customer", d)[UPDATED_AT].tolist() for d in nights(3)]

    second_sink = LocalBlobSink(tmp_path / "dumps_again")
    run(olist_frames, second_sink, start=START, nights=3, stride_days=STRIDE, change_rate=0)
    second_run = [read_dump(second_sink, "customer", d)[UPDATED_AT].tolist() for d in nights(3)]

    assert first_run == second_run
    assert all(pd.Timestamp(ts).year == 2017 for night in first_run for ts in night)


def test_synthetic_product_change_moves_only_the_touched_rows(olist_frames, sink):
    run(olist_frames, sink, start=START, nights=2, stride_days=STRIDE, change_rate=1)
    first, second = nights(2)

    before = read_dump(sink, "product", first).set_index(PRODUCT.key)
    after = read_dump(sink, "product", second).set_index(PRODUCT.key)

    moved = [k for k in after.index if after.loc[k, UPDATED_AT] != before.loc[k, UPDATED_AT]]
    assert len(moved) == 1, f"change-rate 1 should move exactly one row, moved {moved}"

    unchanged = [k for k in after.index if k not in moved]
    assert all(after.loc[k, UPDATED_AT] == before.loc[k, UPDATED_AT] for k in unchanged)


def test_numeric_null_elsewhere_in_the_column_does_not_flip_every_other_row(sink):
    """Regression for the incident found against the real dataset.

    Plain `pd.read_csv` upcasts a WHOLE integer column to float64 the moment
    ANY row in it is null, so night 2's read-back of night 1's dump turns
    `225` into `225.0` for every row in that column, not just the null one.
    A string-based hash sees that as a change on every row, forever.

    Olist's real product_weight_g has scattered nulls; a small fixture with
    no missing numeric values never exercises this shape. This one deliberately
    includes a null so the CSV round-trip actually upcasts the column.
    """
    products = pd.DataFrame(
        [
            ("P1", "cat_a", 225, 10, 10, 10, 1),
            ("P2", "cat_b", None, 12, 12, 12, 1),  # forces the column to float64 on read-back
            ("P3", "cat_c", 500, 20, 20, 20, 2),
        ],
        columns=list(PRODUCT.columns),
    )
    numeric_cols = [c for c in PRODUCT.columns if c not in ("product_id", "product_category_name")]
    products[numeric_cols] = products[numeric_cols].astype("Int64")

    write_night(sink, PRODUCT, products, date(2017, 1, 31))
    write_night(sink, PRODUCT, products.copy(), date(2017, 5, 31))  # identical content, re-derived

    before = read_dump(sink, "product", date(2017, 1, 31)).set_index(PRODUCT.key)
    after = read_dump(sink, "product", date(2017, 5, 31)).set_index(PRODUCT.key)

    for key in ("P1", "P3"):
        assert after.loc[key, UPDATED_AT] == before.loc[key, UPDATED_AT], (
            f"{key} falsely flagged as changed by a dtype artifact, not real content"
        )


def test_attribute_hash_is_stable_across_int_and_float_representations():
    """The unit-level guard behind the regression above."""
    int_row = pd.Series({"product_weight_g": np.int64(225)})
    float_row = pd.Series({"product_weight_g": np.float64(225.0)})
    spec = PRODUCT.__class__(name="probe", key="k", attributes=("product_weight_g",))

    assert attribute_hash(int_row, spec) == attribute_hash(float_row, spec)

    # a genuinely different value must still hash differently
    other_row = pd.Series({"product_weight_g": np.float64(226.0)})
    assert attribute_hash(int_row, spec) != attribute_hash(other_row, spec)

    # a real fraction is not silently rounded away
    frac_row = pd.Series({"product_weight_g": np.float64(225.5)})
    assert attribute_hash(int_row, spec) != attribute_hash(frac_row, spec)


def test_null_category_backfill_is_drawn_from_the_real_gap(products):
    """PROD_NOCAT has a genuinely missing category; filling it is a real change."""
    frame = products[list(PRODUCT.columns)].copy()
    assert frame.set_index(PRODUCT.key).loc["PROD_NOCAT", "product_category_name"] is None or pd.isna(
        frame.set_index(PRODUCT.key).loc["PROD_NOCAT", "product_category_name"]
    )

    changed, count = apply_synthetic_changes(frame, PRODUCT, night=1, rate=4, seed=1)
    assert count == 4
    assert changed.set_index(PRODUCT.key).loc["PROD_NOCAT", "product_category_name"] == "categoria_backfilled"


def test_night_zero_is_a_clean_baseline(olist_frames, sink):
    """No synthetic edits and no deletions on the first dump."""
    frame = olist_frames["products"][list(PRODUCT.columns)].copy()
    unchanged, count = apply_synthetic_changes(frame, PRODUCT, night=0, rate=99, seed=1)
    assert count == 0
    pd.testing.assert_frame_equal(frame, unchanged)
    assert deleted_keys(PRODUCT, ["a", "b", "c"], night=0, per_night=2, seed=1) == set()


# --------------------------------------------------------------------------
# Deletion: absence from the file is the only signal
# --------------------------------------------------------------------------


def test_deleted_rows_vanish_from_the_snapshot_and_stay_gone(olist_frames, sink):
    """This absence is what NMBS DELETE catches and a plain MERGE misses."""
    run(olist_frames, sink, start=START, nights=3, stride_days=STRIDE, change_rate=0, delete_per_night=1)
    first, second, third = nights(3)

    products_n0 = set(read_dump(sink, "product", first)[PRODUCT.key])
    products_n1 = set(read_dump(sink, "product", second)[PRODUCT.key])
    products_n2 = set(read_dump(sink, "product", third)[PRODUCT.key])

    assert len(products_n0) == 4, "night 0 is untouched"
    assert products_n1 < products_n0, "a product left the dump"
    # deletions accumulate: a row removed on night 1 does not come back on night 2
    assert products_n2 <= products_n1


def test_deletion_selection_is_deterministic():
    keys = [f"k{i}" for i in range(20)]
    first = deleted_keys(SELLER, keys, night=3, per_night=2, seed=7)
    second = deleted_keys(SELLER, keys, night=3, per_night=2, seed=7)
    assert first == second
    assert len(first) == 5, "3 nights x 2 would be 6, but the 25% cap of 20 keys is 5"
    assert deleted_keys(SELLER, keys, night=2, per_night=2, seed=7) < first, "cumulative"


def test_deletion_ignores_which_rows_happen_to_exist_tonight():
    """Regression: the pool must be the full key universe, not tonight's frame.

    The customer dimension grows as people place their first order. Sampling
    from the rows present on a given night made the 'cumulative' set recompute
    differently every night, so earlier nights' deletions changed retroactively
    and the dimension drained to zero. docs/incidents.md [2026-09-19].
    """
    universe = [f"k{i}" for i in range(20)]
    stable = deleted_keys(CUSTOMER, universe, night=2, per_night=1, seed=7)

    # the same universe passed in a different order, or with duplicates, is
    # still the same universe and must select the same keys
    assert deleted_keys(CUSTOMER, list(reversed(universe)), night=2, per_night=1, seed=7) == stable
    assert deleted_keys(CUSTOMER, universe + universe, night=2, per_night=1, seed=7) == stable

    # a smaller pool (what "tonight's rows" would have given) selects differently
    assert deleted_keys(CUSTOMER, universe[:8], night=2, per_night=1, seed=7) != stable


def test_deletion_can_never_empty_a_dimension():
    """A drained dimension is a truncation, not a deletion test.

    It would make WHEN NOT MATCHED BY SOURCE DELETE wipe the whole Silver table
    for a reason that has nothing to do with the pattern under test.
    """
    tiny = ["a", "b"]
    assert deleted_keys(SELLER, tiny, night=99, per_night=5, seed=7) == set(), "25% of 2 rounds to 0"

    keys = [f"k{i}" for i in range(20)]
    assert len(deleted_keys(SELLER, keys, night=99, per_night=10, seed=7)) == 5
    assert deleted_keys(SELLER, [], night=3, per_night=1, seed=7) == set()


def test_changes_are_applied_after_deletions_not_before(olist_frames, sink):
    """Regression: a change to a row that is then deleted never reaches the dump.

    With changes applied first, the generator's own summary claimed edits that
    its output did not contain.
    """
    report = run(
        olist_frames, sink, start=START, nights=2, stride_days=STRIDE,
        change_rate=99, delete_per_night=1,
    )
    night_one = {r["dimension"]: r for r in report if r[DUMP_DATE] == nights(2)[1].isoformat()}

    products = night_one["product"]
    assert products["rows"] == 3, "4 products minus the one deleted"
    assert products["synthetic_changes"] == products["rows"], (
        "every reported change must land on a surviving row"
    )


def test_customer_dimension_grows_as_people_place_first_orders(olist_frames, sink):
    """The visible symptom of the deletion bug was a dimension that shrank to zero."""
    run(olist_frames, sink, start=START, nights=5, stride_days=STRIDE, change_rate=0, delete_per_night=1)
    counts = [len(read_dump(sink, "customer", d)) for d in nights(5)]

    assert counts == sorted(counts), f"customer dimension shrank: {counts}"
    assert counts[-1] >= 3, f"expected the dimension to fill up, got {counts}"
    assert min(counts) > 0


def test_dumps_are_full_snapshots_not_deltas(olist_frames, sink):
    """Every dump must carry every live row, or 'absence means deleted' breaks."""
    run(olist_frames, sink, start=START, nights=3, stride_days=STRIDE, change_rate=1)

    for dump_date in nights(3):
        products = read_dump(sink, "product", dump_date)
        assert len(products) == 4, f"{dump_date}: expected all 4 products, got {len(products)}"


def test_partition_layout_is_hive_style_for_auto_loader(olist_frames, sink):
    run(olist_frames, sink, start=START, nights=2, stride_days=STRIDE)
    keys = sink.list_keys("dims/")

    assert f"dims/customer/dump_date={START.isoformat()}/customer.csv" in keys
    assert len(keys) == 2 * 3, "2 nights x 3 dimensions"
    assert all("dump_date=" in k for k in keys)


def test_dump_bytes_are_the_same_on_every_machine(olist_frames, sink, monkeypatch):
    """No carriage return in any dump, even where the platform's line ending is CRLF.

    Session 2 found `to_csv()` defaulting to `os.linesep` when it returns a
    string, so a dump generated on Windows had CRLF and the same dump from Linux
    had LF. `write_night` pins `lineterminator="\n"`. Until Drill 1 nothing
    tested that pin: the sink comparison test hands both sinks the *same*
    string, so a CRLF string stored twice still compares equal. Drill 1 removed
    the pin on purpose and the suite stayed green.

    `os.linesep` is forced to Windows' value, so this fails on a Linux CI
    runner too — the bug is platform-dependent; the test must not be.
    """
    import os

    monkeypatch.setattr(os, "linesep", "\r\n")
    run(olist_frames, sink, start=START, nights=2, stride_days=STRIDE)

    keys = sink.list_keys("dims/")
    assert keys
    for key in keys:
        assert "\r" not in sink.get_text(key), f"{key} contains a carriage return"


# --------------------------------------------------------------------------
# Write-once landing (Session 8)
# --------------------------------------------------------------------------


class CountingSink(LocalBlobSink):
    """A local sink that remembers every key written to it."""

    def __init__(self, root) -> None:
        super().__init__(root)
        self.puts: list[str] = []

    def put_text(self, key: str, text: str) -> None:
        self.puts.append(key)
        super().put_text(key, text)


def snapshot_of(sink: LocalBlobSink) -> dict[str, str]:
    return {key: sink.get_text(key) for key in sink.list_keys("dims/")}


@pytest.fixture
def counting(tmp_path) -> CountingSink:
    return CountingSink(tmp_path / "landing")


def test_a_second_identical_run_writes_nothing(olist_frames, counting):
    """Before Session 8 every run rewrote every night: new modification times,
    and Bronze (allowOverwrites) re-read all of them."""
    run(olist_frames, counting, start=START, nights=3, stride_days=STRIDE)
    counting.puts.clear()

    report = run(olist_frames, counting, start=START, nights=3, stride_days=STRIDE)

    assert counting.puts == []
    assert {row["landed"] for row in report} == {"identical"}


def test_extending_by_one_night_writes_only_that_night(olist_frames, counting):
    run(olist_frames, counting, start=START, nights=2, stride_days=STRIDE)
    counting.puts.clear()

    run(olist_frames, counting, start=START, nights=3, stride_days=STRIDE)

    third = nights(3)[2]
    assert sorted(counting.puts) == sorted(dump_key(d, third) for d in ("customer", "product", "seller"))


def test_a_run_that_would_change_a_landed_night_writes_nothing(olist_frames, counting):
    """The mistake this rule exists for: new arguments, old nights.

    Deletions are cumulative from the second night, so turning them on without
    --delete-from recomputes nights that are already delivered. Before Session 8
    those nights were silently rewritten, and Bronze would have followed.
    """
    run(olist_frames, counting, start=START, nights=3, stride_days=STRIDE, change_rate=0)
    before = snapshot_of(counting)
    counting.puts.clear()

    with pytest.raises(LandedNightChanged) as refused:
        run(
            olist_frames, counting, start=START, nights=4, stride_days=STRIDE,
            change_rate=0, delete_per_night=1,
        )

    assert counting.puts == [], "a refused run must write nothing, not even the new night"
    assert snapshot_of(counting) == before
    assert dump_key("product", nights(3)[1]) in str(refused.value), "the refusal names the night"


def test_delete_from_leaves_earlier_nights_byte_identical(olist_frames, counting, tmp_path):
    """Night 5 on the live landing zone gets deletions; nights 1-4 must not move."""
    run(olist_frames, counting, start=START, nights=3, stride_days=STRIDE)
    before = snapshot_of(counting)
    counting.puts.clear()

    fourth = nights(4)[3]
    report = run(
        olist_frames, counting, start=START, nights=4, stride_days=STRIDE,
        delete_per_night=1, delete_from=fourth,
    )

    assert sorted(counting.puts) == sorted(dump_key(d, fourth) for d in ("customer", "product", "seller"))
    assert {k: v for k, v in snapshot_of(counting).items() if k in before} == before
    night4 = {r["dimension"]: r for r in report if r[DUMP_DATE] == fourth.isoformat()}
    assert night4["product"]["deleted_vs_previous"] == 1
    assert night4["seller"]["deleted_vs_previous"] == 0, "2 sellers: the 25% cap rounds to 0"

    # The whole landed history is reproducible from ONE command, from nothing.
    fresh = LocalBlobSink(tmp_path / "from_scratch")
    run(
        olist_frames, fresh, start=START, nights=4, stride_days=STRIDE,
        delete_per_night=1, delete_from=fourth,
    )
    assert snapshot_of(fresh) == snapshot_of(counting)


def test_redeliver_rewrites_only_the_named_night(olist_frames, counting):
    """A correction is for one named night; every other landed night stays refused."""
    run(olist_frames, counting, start=START, nights=3, stride_days=STRIDE, change_rate=0)
    before = snapshot_of(counting)
    second, third = nights(3)[1], nights(3)[2]

    # change_rate=1 alters products/sellers on nights 2 and 3; naming only night 2 is not enough
    with pytest.raises(LandedNightChanged):
        run(
            olist_frames, counting, start=START, nights=3, stride_days=STRIDE,
            change_rate=1, redeliver=[second],
        )
    assert snapshot_of(counting) == before

    counting.puts.clear()
    report = run(
        olist_frames, counting, start=START, nights=3, stride_days=STRIDE,
        change_rate=1, redeliver=[second, third],
    )
    expected = sorted(dump_key(d, n) for d in ("product", "seller") for n in (second, third))
    assert sorted(counting.puts) == expected
    first_night = nights(3)[0].isoformat()
    assert {r["landed"] for r in report if r[DUMP_DATE] == first_night} == {"identical"}


def test_first_night_holds_deletions_back_until_it():
    keys = [f"k{i}" for i in range(40)]
    assert deleted_keys(SELLER, keys, night=2, per_night=2, seed=7, first_night=3) == set()
    only_third = deleted_keys(SELLER, keys, night=3, per_night=2, seed=7, first_night=3)
    assert len(only_third) == 2, "one night's worth, not three nights' cumulative"
    assert deleted_keys(SELLER, keys, night=4, per_night=2, seed=7, first_night=3) > only_third


# --------------------------------------------------------------------------
# Known source defect, pinned on purpose (incidents.md 2026-09-30)
# --------------------------------------------------------------------------


def test_zip_prefix_is_dumped_without_its_leading_zero(tmp_path, customers, sink):
    """Olist's `01409` lands as `1409`: the contract reads zips as numbers.

    This pins the defect rather than fixing it. Silver restores the five digits;
    changing the generator mid-stream would make every leading-zero customer
    look "changed" on the next night and open fake SCD2 versions in Gold
    (decisions.md, Session 8, "Zip prefixes are restored ... in Silver"). If
    this test fails because someone fixed the generator, read that entry first.
    """
    raw = tmp_path / "raw_zip"
    raw.mkdir()
    frame = customers.astype({"customer_zip_code_prefix": "string"})
    frame["customer_zip_code_prefix"] = "01409"
    frame.to_csv(raw / olist.TABLES["customers"].filename, index=False)

    loaded = olist.load("customers", raw)
    snapshot = customer_snapshot(customer_timeline(loaded, olist_orders_for(loaded)), datetime(2017, 12, 31))
    write_night(sink, CUSTOMER, snapshot, date(2017, 12, 31))

    dumped = pd.read_csv(io.StringIO(sink.get_text(dump_key("customer", date(2017, 12, 31)))), dtype=str)
    assert set(dumped["customer_zip_code_prefix"]) == {"1409"}


def olist_orders_for(customers: pd.DataFrame) -> pd.DataFrame:
    """One order per customer_id, all on the same day — enough for a snapshot."""
    return pd.DataFrame(
        {
            "customer_id": customers["customer_id"],
            "order_purchase_timestamp": pd.Timestamp("2017-06-01 10:00:00"),
        }
    )


# --------------------------------------------------------------------------
# Customer contact lane — synthetic PII (Session 12)
# --------------------------------------------------------------------------


def test_contact_lane_is_off_unless_asked_for(olist_frames, sink):
    run(olist_frames, sink, start=START, nights=3, stride_days=STRIDE)

    assert sink.list_keys("pii/") == []


def test_turning_the_contact_lane_on_leaves_every_dims_file_byte_identical(olist_frames, counting):
    """The live landing zone already holds five nights. Adding the lane must write only pii/ files —
    a dims night that changed would be refused, and Bronze would follow it if it were not."""
    run(olist_frames, counting, start=START, nights=3, stride_days=STRIDE)
    before = snapshot_of(counting)
    counting.puts.clear()

    second = nights(3)[1]
    run(olist_frames, counting, start=START, nights=3, stride_days=STRIDE, pii_from=second)

    assert snapshot_of(counting) == before, "no dims file may move"
    assert sorted(counting.puts) == sorted(dump_key("customer_contact", n) for n in nights(3)[1:])
    assert dump_key("customer_contact", second).startswith("pii/customer_contact/dump_date=")


def test_contact_rows_are_exactly_tonights_customers(olist_frames, sink):
    last = nights(4)[3]
    run(
        olist_frames, sink, start=START, nights=4, stride_days=STRIDE,
        delete_per_night=1, delete_from=last, pii_from=START,
    )

    for night in nights(4):
        customers = read_dump(sink, "customer", night)
        contacts = read_dump(sink, "customer_contact", night)
        assert sorted(contacts["customer_unique_id"]) == sorted(customers["customer_unique_id"])
        assert contacts["customer_unique_id"].is_unique


def test_contact_values_are_synthetic_on_their_face_and_never_change(olist_frames, sink):
    run(olist_frames, sink, start=START, nights=4, stride_days=STRIDE, pii_from=START)
    first = read_dump(sink, "customer_contact", nights(4)[0])
    last = read_dump(sink, "customer_contact", nights(4)[3])

    assert last["customer_email"].str.endswith("@example.com").all(), "RFC 2606: delivers nowhere"
    assert last["customer_phone"].str.startswith("+55 00 9").all(), "area code 00 does not exist"
    assert last["customer_email"].is_unique

    # A person's contact row is the same every night, timestamp included: nothing changed.
    common = first.merge(last, on="customer_unique_id", suffixes=("_1", "_4"))
    assert not common.empty
    for column in ("customer_name", "customer_email", "customer_phone", UPDATED_AT):
        assert (common[f"{column}_1"] == common[f"{column}_4"]).all(), column


def test_contact_dim_updated_at_is_the_first_order_not_the_latest(olist_frames, sink):
    """CU_MOVER changes city over time: the customer row's timestamp moves, the contact row's must not."""
    run(olist_frames, sink, start=START, nights=4, stride_days=STRIDE, pii_from=START)
    last = nights(4)[3]

    customer = read_dump(sink, "customer", last).set_index("customer_unique_id")
    contact = read_dump(sink, "customer_contact", last).set_index("customer_unique_id")
    timeline = customer_timeline(olist_frames["customers"], olist_frames["orders"])
    first_order = timeline[timeline["customer_unique_id"] == CU_MOVER]["observed_at"].min()

    assert contact.loc[CU_MOVER, UPDATED_AT] == first_order
    assert customer.loc[CU_MOVER, UPDATED_AT] > first_order, "the fixture must really move"


def test_synthetic_contact_is_deterministic_and_ascii_in_the_address():
    from generators.dim_dumps import synthetic_contact

    assert synthetic_contact("abc") == synthetic_contact("abc")
    assert synthetic_contact("abc") != synthetic_contact("abd")
    for person in (f"p{i}" for i in range(500)):
        _, email, _ = synthetic_contact(person)
        assert email.isascii(), email
