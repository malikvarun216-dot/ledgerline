"""The delivery-days model's shared code (databricks/ml/_delivery_days), tested offline.

The notebook file is plain Python (its Databricks markup is comments), so it is loaded by path. Every
function in it is pandas / numpy only: what decides which labels may train a model, and how the model
turns whatever types it is given into numbers, is checked here without Spark or scikit-learn.
"""

from __future__ import annotations

import importlib.util
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PATH = Path(__file__).resolve().parent.parent / "databricks" / "ml" / "_delivery_days.py"


def _load():
    spec = importlib.util.spec_from_file_location("_delivery_days", PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


dd = _load()


def _row(**over):
    base = {
        "promised_days": 20, "total_price": Decimal("99.90"), "total_freight": Decimal("15.10"),
        "n_items": 1, "purchase_month": 11, "purchase_weekday": 6,
        "seller_deliveries": 12, "seller_late_rate": 0.25, "seller_avg_days": 9.5,
        "customer_state": "RJ", "seller_state": "SP",
    }
    base.update(over)
    return base


# --- model_inputs: the model's own first step ---------------------------------------------------


def test_inputs_are_picked_by_name_in_a_fixed_order_and_extra_columns_ignored():
    X = pd.DataFrame([_row(order_id="o1", actual_days=7)])
    out = dd.model_inputs(X)
    assert list(out.columns) == dd.MODEL_INPUTS
    assert (out.dtypes == "float64").all()


def test_decimal_prices_and_nullable_int_with_na_both_become_float():
    # The S12b failure: score_batch hands a missing BIGINT over as Int64 + pd.NA.
    X = pd.DataFrame([_row(), _row(seller_deliveries=None, total_price=None)])
    X["seller_deliveries"] = X["seller_deliveries"].astype("Int64")
    out = dd.model_inputs(X)
    assert out.loc[0, "total_price"] == pytest.approx(99.90)
    assert np.isnan(out.loc[1, "seller_deliveries"])
    assert np.isnan(out.loc[1, "total_price"])


def test_states_map_to_fixed_codes_and_unknown_becomes_missing():
    X = pd.DataFrame([_row(), _row(customer_state=None), _row(seller_state="sp")])
    out = dd.model_inputs(X)
    assert out.loc[0, "customer_state"] == dd.STATES.index("RJ")
    assert out.loc[0, "seller_state"] == dd.STATES.index("SP")
    assert np.isnan(out.loc[1, "customer_state"])
    assert np.isnan(out.loc[2, "seller_state"])  # lowercase is not a code: missing, not an error


def test_same_state_is_one_zero_or_missing():
    X = pd.DataFrame([_row(customer_state="SP"), _row(), _row(customer_state=None)])
    out = dd.model_inputs(X)
    assert out["same_state"].tolist()[:2] == [1.0, 0.0]
    assert np.isnan(out.loc[2, "same_state"])


def test_categorical_positions_point_at_the_state_columns():
    assert [dd.MODEL_INPUTS[i] for i in dd.CATEGORICAL] == dd.STATE_COLUMNS
    assert len(dd.STATES) == 27 and len(set(dd.STATES)) == 27


# --- training_mask: which labels may train a model at the cutoff ---------------------------------

CUTOFF = "2018-01-01"


def _orders():
    # name: (placed, delivered)
    rows = {
        "old_fast": ("2017-06-01", "2017-06-10"),
        "recent_fast": ("2017-12-20", "2017-12-27"),
        "recent_slow": ("2017-12-20", "2018-01-10"),  # label exists only after the cutoff
        "at_window_edge": ("2017-11-21", "2017-12-01"),  # placed exactly cutoff - 41 days
        "arrives_at_cutoff": ("2017-10-01", "2018-01-01"),
        "test_order": ("2018-01-05", "2018-01-15"),
    }
    df = pd.DataFrame(rows, index=["created_at", "delivered_at"]).T
    return pd.to_datetime(df["created_at"]), pd.to_datetime(df["delivered_at"])


def _chosen(mask):
    return sorted(mask[mask].index)


def test_placed_rule_uses_a_label_from_after_the_cutoff():
    created, delivered = _orders()
    chosen = _chosen(dd.training_mask(created, delivered, CUTOFF, "placed"))
    assert "recent_slow" in chosen and "arrives_at_cutoff" in chosen
    assert "test_order" not in chosen


def test_delivered_rule_keeps_the_recent_fast_order_but_not_the_recent_slow_one():
    created, delivered = _orders()
    chosen = _chosen(dd.training_mask(created, delivered, CUTOFF, "delivered"))
    assert chosen == ["at_window_edge", "old_fast", "recent_fast"]  # arrival AT the cutoff is not known


def test_matured_rule_drops_the_recent_window_and_is_strict_at_its_edge():
    created, delivered = _orders()
    chosen = _chosen(dd.training_mask(created, delivered, CUTOFF, "matured", window_days=41))
    assert chosen == ["old_fast"]
    chosen = _chosen(dd.training_mask(created, delivered, CUTOFF, "matured", window_days=40))
    assert chosen == ["at_window_edge", "old_fast"]


def test_matured_rule_needs_a_window_and_an_unknown_rule_is_refused():
    created, delivered = _orders()
    with pytest.raises(ValueError, match="window_days"):
        dd.training_mask(created, delivered, CUTOFF, "matured")
    with pytest.raises(ValueError, match="unknown rule"):
        dd.training_mask(created, delivered, CUTOFF, "all")


def test_maturity_window_reads_only_orders_settled_long_before_the_cutoff():
    created = pd.to_datetime(["2017-01-01"] * 100 + ["2017-12-01", "2017-03-01"])
    delivered = pd.to_datetime(["2017-01-10"] * 100 + ["2017-12-05", "2018-03-01"])
    days = [*range(1, 101), 500, 365]  # the last two: too recent, and arrived after the cutoff
    created, delivered = pd.Series(created), pd.Series(delivered)
    assert dd.maturity_window(created, delivered, days, CUTOFF, q=0.99) == 100
    with pytest.raises(ValueError, match="no settled orders"):
        dd.maturity_window(created, delivered, days, "2017-02-01")


# --- the numbers every model is read by ---------------------------------------------------------


def test_errors_mae_bias_and_within_three_days():
    e = dd.errors([10, 10, 10, 10], [7, 10, 14, 13])
    assert e["mae"] == pytest.approx(2.5)
    assert e["bias"] == pytest.approx(1.0)  # slower than reality on average
    assert e["within_3_days"] == pytest.approx(0.75)


def test_promise_is_late_only_on_a_later_day():
    r = dd.promise_report([10, 11, 9, 20], [10, 10, 10, 30])
    assert r["late_rate"] == pytest.approx(0.25)  # on the promised day is on time
    assert r["mean_promised_days"] == pytest.approx(15.0)


def test_route_median_falls_back_to_the_overall_median_for_an_unseen_route():
    train = pd.DataFrame({
        "seller_state": ["SP", "SP", "SP", "MG"],
        "customer_state": ["RJ", "RJ", "RJ", "BA"],
        "actual_days": [5, 7, 30, 20],
    })
    test = pd.DataFrame({"seller_state": ["SP", "PR", None], "customer_state": ["RJ", "AM", "RJ"]},
                        index=[10, 11, 12])
    got = dd.route_median(train, test)
    assert got.tolist() == [7.0, 13.5, 13.5]
    assert list(got.index) == [10, 11, 12]
