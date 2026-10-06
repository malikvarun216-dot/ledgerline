# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Score `workspace.ml.late_delivery@champion` — in a session of its own (Session 12b)
# MAGIC
# MAGIC Production scores a registered model somewhere other than where it was trained. This notebook
# MAGIC does exactly that, and checks two things:
# MAGIC
# MAGIC 1. **What each registered version contains** — its scikit-learn pipeline's steps, read from the
# MAGIC    registry's own copy (in the training session, `score_batch` behaved like an older version).
# MAGIC 2. **Scoring through Feature Engineering** — `score_batch` is given only the test orders' spine and
# MAGIC    looks each seller up as of the order's time itself; every prediction must equal the training
# MAGIC    notebook's (`late_delivery` saved them in `workspace.ml.late_delivery_test_orders`).

# COMMAND ----------

# MAGIC %pip install --quiet databricks-feature-engineering scikit-learn

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os

import mlflow
from databricks.feature_engineering import FeatureEngineeringClient
from mlflow import MlflowClient

MODEL = "workspace.ml.late_delivery"
TEST = "workspace.ml.late_delivery_test_orders"
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
spine = test.drop("step6_prediction")  # ids, time and order columns only; seller features looked up
scored = fe.score_batch(model_uri=f"models:/{MODEL}@champion", df=spine)
trained = test.select("order_id", "step6_prediction")
both = scored.select("order_id", "prediction").join(trained, "order_id", "full_outer")
summary = both.selectExpr(
    "count(*) AS orders",
    "count_if(prediction = step6_prediction) AS same",
    "count_if(prediction IS NULL OR step6_prediction IS NULL) AS one_side_only",
    "sum(prediction) AS predicted_late",
).first()
print(f"score_batch: {summary.orders:,} test orders; same prediction as training: {summary.same:,}; "
      f"on one side only: {summary.one_side_only}; predicted late: {summary.predicted_late}")
assert summary.same == summary.orders, "scoring disagrees with training"