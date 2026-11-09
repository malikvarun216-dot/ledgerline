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
