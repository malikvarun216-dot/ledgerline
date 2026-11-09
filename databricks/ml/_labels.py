# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Labels have a time — shared code (Session 33, moved here Session 34)
# MAGIC
# MAGIC Loaded with `%run ./_labels` by `delivery_days` and `late_delivery`. **Defines functions only —
# MAGIC running it writes nothing.** Plain pandas, no Spark, so it is tested offline (`tests/test_labels.py`).
# MAGIC
# MAGIC A feature is fixed as of the order (S12b). A **label** also becomes known at a moment, and a model
# MAGIC built at a cutoff may only learn labels known by then (incidents.md 2026-11-10).
# MAGIC
# MAGIC | piece | what it decides |
# MAGIC |---|---|
# MAGIC | `training_mask` | which orders may train a model at a cutoff: the three label rules |
# MAGIC | `maturity_window` | how long to wait before a label counts as settled |
# MAGIC | `late_label_known_at` | when "late or not" is decided — earlier than the arrival for a late parcel |

# COMMAND ----------

import math

import pandas as pd

# "delivered" = the label is known before the cutoff. Named for delivery days, where a label is known
# on arrival; given another `known_at` (a yes/no decided earlier) it means "known" all the same.
RULES = ("placed", "delivered", "matured")


def training_mask(created_at, known_at, cutoff, rule, window_days=None):
    """Which orders may train a model built at `cutoff`, under one of three label rules.

    `known_at` is when each order's label became known: the arrival for delivery days,
    `late_label_known_at` for late-or-not.

    placed     placed before the cutoff — includes labels that only exist after it (the leak);
    delivered  label known before the cutoff — but the newest orders keep only the labels that are
               known early (fast parcels; on-time ones for late-or-not), so they lean one way;
    matured    placed at least `window_days` before the cutoff AND known before it — known labels,
               and old enough that almost every order's label is in.
    """
    cutoff = pd.Timestamp(cutoff)
    if rule == "placed":
        return created_at < cutoff
    known = known_at < cutoff
    if rule == "delivered":
        return known
    if rule == "matured":
        if window_days is None:
            raise ValueError("rule 'matured' needs window_days")
        return (created_at < cutoff - pd.Timedelta(days=window_days)) & known
    raise ValueError(f"unknown rule {rule!r}; expected one of {RULES}")


def maturity_window(created_at, known_at, days_to_known, cutoff, q=0.99, settled_days=180):
    """Days to wait before a label counts as settled: the q-quantile of days from order to label among
    orders that were placed more than `settled_days` before the cutoff and whose label was known by it
    — orders whose outcome was long known at the cutoff, so the window itself uses no label from after
    it."""
    cutoff = pd.Timestamp(cutoff)
    settled = (created_at < cutoff - pd.Timedelta(days=settled_days)) & (known_at < cutoff)
    days = pd.to_numeric(pd.Series(days_to_known)[settled.to_numpy()], errors="coerce").dropna()
    if days.empty:
        raise ValueError("no settled orders before the cutoff")
    return math.ceil(float(days.quantile(q)))


def late_label_known_at(delivered_at, promised_at):
    """When "late or not" is decided. Late = arrived on a later calendar day than promised (S12b's rule,
    Gold's `delivery_promise.is_late`), so:

    on time  is known when the parcel arrives (on or before the promised day);
    late     is known the moment the promised day ends — a parcel still on the road then is late
             whenever it arrives, and one never delivered (NaT) is late from then too.

    So the label is known at the earlier of the arrival and midnight after the promised day.
    """
    delivered_at = pd.to_datetime(pd.Series(delivered_at))
    deadline = pd.to_datetime(pd.Series(promised_at, index=delivered_at.index)).dt.normalize()
    deadline = deadline + pd.Timedelta(days=1)
    return delivered_at.where(delivered_at < deadline, deadline)