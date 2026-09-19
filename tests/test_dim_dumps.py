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

from generators._common import LocalBlobSink
from generators.dim_dumps import (
    CUSTOMER,
    DUMP_DATE,
    PRODUCT,
    SELLER,
    UPDATED_AT,
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
