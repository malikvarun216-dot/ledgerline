"""The label rules shared by delivery_days and late_delivery (databricks/ml/_labels), tested offline.

A label becomes known at a moment, like a feature does. These functions decide which orders may train a
model built at a cutoff, and when a late-or-not label is decided. The notebook file is plain Python (its
Databricks markup is comments), so it is loaded by path; it is pandas only.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest

PATH = Path(__file__).resolve().parent.parent / "databricks" / "ml" / "_labels.py"


def _load():
    spec = importlib.util.spec_from_file_location("_labels", PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


lb = _load()


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
    chosen = _chosen(lb.training_mask(created, delivered, CUTOFF, "placed"))
    assert "recent_slow" in chosen and "arrives_at_cutoff" in chosen
    assert "test_order" not in chosen


def test_delivered_rule_keeps_the_recent_fast_order_but_not_the_recent_slow_one():
    created, delivered = _orders()
    chosen = _chosen(lb.training_mask(created, delivered, CUTOFF, "delivered"))
    assert chosen == ["at_window_edge", "old_fast", "recent_fast"]  # arrival AT the cutoff is not known


def test_matured_rule_drops_the_recent_window_and_is_strict_at_its_edge():
    created, delivered = _orders()
    chosen = _chosen(lb.training_mask(created, delivered, CUTOFF, "matured", window_days=41))
    assert chosen == ["old_fast"]
    chosen = _chosen(lb.training_mask(created, delivered, CUTOFF, "matured", window_days=40))
    assert chosen == ["at_window_edge", "old_fast"]


def test_matured_rule_needs_a_window_and_an_unknown_rule_is_refused():
    created, delivered = _orders()
    with pytest.raises(ValueError, match="window_days"):
        lb.training_mask(created, delivered, CUTOFF, "matured")
    with pytest.raises(ValueError, match="unknown rule"):
        lb.training_mask(created, delivered, CUTOFF, "all")


def test_maturity_window_reads_only_orders_settled_long_before_the_cutoff():
    created = pd.to_datetime(["2017-01-01"] * 100 + ["2017-12-01", "2017-03-01"])
    delivered = pd.to_datetime(["2017-01-10"] * 100 + ["2017-12-05", "2018-03-01"])
    days = [*range(1, 101), 500, 365]  # the last two: too recent, and arrived after the cutoff
    created, delivered = pd.Series(created), pd.Series(delivered)
    assert lb.maturity_window(created, delivered, days, CUTOFF, q=0.99) == 100
    with pytest.raises(ValueError, match="no settled orders"):
        lb.maturity_window(created, delivered, days, "2017-02-01")


# --- late_label_known_at: when late-or-not is decided ---------------------------------------------


def _decided(delivered, promised):
    return lb.late_label_known_at(pd.to_datetime(pd.Series(delivered)), pd.to_datetime(pd.Series(promised)))


def test_on_time_is_known_on_arrival():
    out = _decided(["2017-12-20 15:30"], ["2017-12-22"])
    assert out[0] == pd.Timestamp("2017-12-20 15:30")


def test_arrival_late_in_the_promised_day_is_still_on_time_and_known_on_arrival():
    out = _decided(["2017-12-22 23:59"], ["2017-12-22"])
    assert out[0] == pd.Timestamp("2017-12-22 23:59")


def test_late_is_known_at_midnight_after_the_promised_day_not_on_arrival():
    out = _decided(["2018-01-10 09:00"], ["2017-12-22"])
    assert out[0] == pd.Timestamp("2017-12-23")


def test_a_promise_with_a_time_counts_by_its_day():
    out = _decided(["2018-01-10 09:00"], ["2017-12-22 18:00"])
    assert out[0] == pd.Timestamp("2017-12-23")


def test_never_delivered_is_decided_late_when_the_promise_passes():
    out = _decided([None], ["2017-12-22"])
    assert out[0] == pd.Timestamp("2017-12-23")


def test_decided_admits_a_late_parcel_still_on_the_road_that_delivered_drops():
    # Placed 2017-11-01, promised 2017-11-25, arrived 2018-01-10: on the road at the cutoff, but late
    # since 2017-11-26. The other: placed 2017-12-28, promised 2018-01-20, arrived 2018-01-05 — on time,
    # unknown at the cutoff under both rules.
    created = pd.to_datetime(pd.Series(["2017-11-01", "2017-12-28"]))
    delivered = pd.to_datetime(pd.Series(["2018-01-10", "2018-01-05"]))
    promised = pd.to_datetime(pd.Series(["2017-11-25", "2018-01-20"]))
    decided = lb.late_label_known_at(delivered, promised)
    assert list(lb.training_mask(created, delivered, CUTOFF, "delivered")) == [False, False]
    assert list(lb.training_mask(created, decided, CUTOFF, "delivered")) == [True, False]
    assert list(lb.training_mask(created, decided, CUTOFF, "matured", window_days=45)) == [True, False]


def test_rules_keep_their_names():
    assert lb.RULES == ("placed", "delivered", "matured")  # MLflow runs since S33 log these names
