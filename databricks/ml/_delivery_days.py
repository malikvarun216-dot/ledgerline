# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Delivery days — shared code (Session 33)
# MAGIC
# MAGIC Loaded with `%run ./_delivery_days` by `delivery_days`. **Defines functions only — running it
# MAGIC writes nothing.** Plain pandas and numpy, no Spark and no scikit-learn, so the same file is tested
# MAGIC offline (`tests/test_delivery_days.py`).
# MAGIC
# MAGIC | piece | what it decides |
# MAGIC |---|---|
# MAGIC | `model_inputs` | the model's own first step: picks its inputs by name and turns them into numbers |
# MAGIC | `errors`, `promise_report`, `route_median` | the numbers every model and baseline is read by |
# MAGIC
# MAGIC The label rules (`training_mask`, `maturity_window`) moved to `_labels` in Session 34, shared with
# MAGIC `late_delivery`.

# COMMAND ----------

import numpy as np
import pandas as pd

# The 27 Brazilian federative units. A fixed list, not an encoder fitted on training data: the model
# turns a state into the same number in the notebook, in score_batch and next year, and a state it has
# never seen becomes "missing" instead of an error (S12b's rule: the model normalises its own inputs).
STATES = (
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA",
    "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
)
STATE_CODE = {s: float(i) for i, s in enumerate(STATES)}

# Known at the moment of purchase, carried by the spine.
ORDER_NUMERIC = [
    "promised_days", "total_price", "total_freight", "n_items", "purchase_month", "purchase_weekday",
]
STATE_COLUMNS = ["customer_state", "seller_state"]
# Looked up as of the purchase time, from the two feature tables.
SELLER_FEATURES = ["seller_deliveries", "seller_late_rate", "seller_avg_days"]
MODEL_INPUTS = [*ORDER_NUMERIC, *SELLER_FEATURES, *STATE_COLUMNS, "same_state"]
# Positions of the state columns in MODEL_INPUTS, for HistGradientBoosting's categorical_features.
CATEGORICAL = [MODEL_INPUTS.index(c) for c in STATE_COLUMNS]


def model_inputs(X):
    """Only the model's inputs, by name, as float64 — whatever types the caller passes.

    In the notebook `toPandas()` gives Decimal objects for prices and float NaN for a missing count;
    inside score_batch's Spark UDF a missing BIGINT arrives as pandas' nullable Int64 with pd.NA, which
    scikit-learn refuses (incidents.md 2026-10-06, S12b). Converting here makes both paths identical.
    States become codes from STATES; anything else (None, a typo) becomes NaN, which the trees treat as
    missing. `same_state` is NaN when either side is unknown.
    """
    out = pd.DataFrame(index=X.index)
    for c in [*ORDER_NUMERIC, *SELLER_FEATURES]:
        out[c] = pd.to_numeric(X[c], errors="coerce").astype("float64")
    for c in STATE_COLUMNS:
        out[c] = X[c].map(STATE_CODE).astype("float64")
    known = out["customer_state"].notna() & out["seller_state"].notna()
    same = (out["customer_state"] == out["seller_state"]).astype("float64")
    out["same_state"] = same.where(known)
    return out[MODEL_INPUTS]


def errors(actual, predicted):
    """MAE in days, bias (positive = the forecast was slower than reality), and the share within 3
    days. One definition for every model and baseline."""
    a = np.asarray(actual, dtype="float64")
    p = np.asarray(predicted, dtype="float64")
    e = p - a
    return {"mae": float(np.mean(np.abs(e))), "bias": float(np.mean(e)),
            "within_3_days": float(np.mean(np.abs(e) <= 3))}


def promise_report(actual_days, promised_days):
    """A promise is late when the parcel arrives on a later calendar day than promised (the same rule
    as Gold's `delivery_promise.is_late`). Returns the late share and the average days promised."""
    a = np.asarray(actual_days, dtype="float64")
    p = np.asarray(promised_days, dtype="float64")
    return {"late_rate": float(np.mean(a > p)), "mean_promised_days": float(np.mean(p))}


def route_median(train, test):
    """Baseline: the median delivery days of the route (seller state to customer state) in the
    training rows; a route never seen there falls back to the overall training median."""
    overall = float(train["actual_days"].median())
    by_route = train.groupby(["seller_state", "customer_state"])["actual_days"].median()
    keys = pd.MultiIndex.from_frame(test[["seller_state", "customer_state"]])
    found = pd.Series(by_route.reindex(keys).to_numpy(), index=test.index, dtype="float64")
    return found.fillna(overall)