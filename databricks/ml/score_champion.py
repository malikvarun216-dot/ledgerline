# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Score a registered model's `@champion` — in a session of its own (Session 12b; widgets S33)
# MAGIC
# MAGIC Production scores a registered model somewhere other than where it was trained. This notebook
# MAGIC does exactly that, and checks two things:
# MAGIC
# MAGIC 1. **What each registered version contains** — its scikit-learn pipeline's steps, read from the
# MAGIC    registry's own copy (in the training session, `score_batch` behaved like an older version).
# MAGIC 2. **Scoring through Feature Engineering** — `score_batch` is given only the test orders' spine and
# MAGIC    looks each seller up as of the order's time itself; every prediction must equal the training
# MAGIC    notebook's (`late_delivery` saved them in `workspace.ml.late_delivery_test_orders`).
# MAGIC
# MAGIC Widgets (Session 33) pick the model; the defaults are S12b's. For the delivery-days model:
# MAGIC `model` = `workspace.ml.delivery_days`, `test_table` = `workspace.ml.delivery_days_test_orders`,
# MAGIC `expected_column` = `trained_prediction`. A regression's predictions are compared within 1e-9.

# COMMAND ----------

# MAGIC %pip install --quiet databricks-feature-engineering scikit-learn

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os

import mlflow
from databricks.feature_engineering import FeatureEngineeringClient
from mlflow import MlflowClient
from pyspark.sql import functions as F

dbutils.widgets.text("model", "workspace.ml.late_delivery")
dbutils.widgets.text("test_table", "workspace.ml.late_delivery_test_orders")
dbutils.widgets.text("expected_column", "step6_prediction")
MODEL = dbutils.widgets.get("model")
TEST = dbutils.widgets.get("test_table")
EXPECTED = dbutils.widgets.get("expected_column")
mlflow.set_registry_uri("databricks-uc")
client = MlflowClient(registry_uri="databricks-uc")

champion = client.get_model_version_by_alias(MODEL, "champion")
print(f"@champion = version {champion.version} (run {champion.run_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What each version contains

# COMMAND ----------


def pipeline_steps(version):
    """Download the version and load every scikit-learn model inside it (Feature Engineering wraps the
    raw model in its own package); return each one's step names and classes."""
    root = mlflow.artifacts.download_artifacts(f"models:/{MODEL}/{version}")
    found = []
    for folder, _, files in os.walk(root):
        if "MLmodel" in files and "sklearn" in open(os.path.join(folder, "MLmodel")).read():
            raw = mlflow.sklearn.load_model(folder)
            steps = [f"{n}:{type(s).__name__}" for n, s in getattr(raw, "steps", [])]
            found.append((os.path.relpath(folder, root), steps))
    return found


for v in sorted(int(m.version) for m in client.search_model_versions(f"name = '{MODEL}'")):
    print(f"version {v}: {pipeline_steps(v)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Score `@champion` through Feature Engineering, compare with training

# COMMAND ----------

fe = FeatureEngineeringClient()
test = spark.table(TEST)
spine = test.drop(EXPECTED)  # ids, time and order columns only; seller features looked up
scored = fe.score_batch(model_uri=f"models:/{MODEL}@champion", df=spine)
trained = test.select("order_id", F.col(EXPECTED).alias("expected"))
both = scored.select("order_id", "prediction").join(trained, "order_id", "full_outer")
summary = both.selectExpr(
    "count(*) AS orders",
    "count_if(abs(prediction - expected) <= 1e-9) AS same",
    "count_if(prediction IS NULL OR expected IS NULL) AS one_side_only",
    "sum(prediction) AS prediction_sum",
    "avg(prediction) AS prediction_mean",
).first()
print(f"score_batch ({MODEL}): {summary.orders:,} test orders; "
      f"same prediction as training: {summary.same:,}; on one side only: {summary.one_side_only}; "
      f"sum of predictions: {summary.prediction_sum} (for late_delivery = predicted late); "
      f"mean: {summary.prediction_mean}")
assert summary.same == summary.orders, "scoring disagrees with training"