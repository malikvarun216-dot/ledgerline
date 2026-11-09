# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Late delivery — point-in-time features, MLflow, a model in Unity Catalog (Session 12b)
# MAGIC
# MAGIC **The question:** at the moment an order is placed, will it arrive after the date promised to the
# MAGIC customer? Both dates are real Olist data (decisions.md, Session 12b).
# MAGIC
# MAGIC **The trap this notebook is about:** the best feature is the seller's late-delivery rate. Asked
# MAGIC "as of when?", the easy answer — over all time — includes deliveries that finished *after* the
# MAGIC order was placed, the order's own among them. A model trained on that has seen the future; it
# MAGIC scores well in the notebook and worse in use. **Leakage is a join bug.** The fix is a
# MAGIC **point-in-time join**: each order sees the seller only as they were when it was placed.
# MAGIC
# MAGIC | step | what | checked against |
# MAGIC |---|---|---|
# MAGIC | 1 | spine: delivered orders, the time of prediction, the label | counts |
# MAGIC | 2 | feature table: one row per seller per delivery completion, keyed `(seller_id, feature_ts)` | — |
# MAGIC | 3 | the as-of join by hand | the answer key |
# MAGIC | 4 | the same join by Feature Engineering (`create_training_set`) | step 3, row for row |
# MAGIC | 5 | the leaky all-time join | — |
# MAGIC | 6 | **which orders may train** (S34): four label rules; a shuffled refit | the truth; AUC |
# MAGIC | 6b | the feature leak: honest vs leaky, both on matured rows | AUC: honest / leaky / in use |
# MAGIC | 7 | register the matured, point-in-time model in UC, alias `champion` | step 6's AUC |
# MAGIC
# MAGIC **The second trap (found Session 33, fixed Session 34):** the label has a time too. Until Session 34
# MAGIC this notebook trained on every order *placed* before the cutoff — 2,496 of them were still on the
# MAGIC road that day (incidents.md 2026-11-10). Step 6 trains only on labels decided by the cutoff, from
# MAGIC orders old enough that almost all labels are in (decisions.md, Session 34).
# MAGIC
# MAGIC Writes only to `workspace.ml`. Safe to "Run all" again: tables are rebuilt from Silver, each run
# MAGIC adds MLflow runs and one model version. Rows are sorted before every fit, so a re-run trains the same
# MAGIC model (S33: Spark's row order fed early stopping).

# COMMAND ----------

# MAGIC %pip install --quiet databricks-feature-engineering scikit-learn

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_labels

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer

ML = "workspace.ml"
FEATURES = f"{ML}.seller_delivery_features"
MODEL = f"{ML}.late_delivery"
CUTOFF = "2018-01-01"  # train on orders placed before, test on orders placed after
SELLER_FEATURES = ["seller_deliveries", "seller_late", "seller_late_rate"]
ORDER_FEATURES = ["promised_days", "total_price", "total_freight", "n_items"]
MODEL_INPUTS = [*ORDER_FEATURES, "seller_deliveries", "seller_late_rate"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ML} COMMENT 'Session 12b: features and models'")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The spine — one row per delivered order, at the moment it was placed

# COMMAND ----------

orders = spark.table("workspace.silver.orders").where(
    "status = 'delivered' AND created_at IS NOT NULL AND delivered_at IS NOT NULL "
    "AND estimated_delivery_at IS NOT NULL"
)
items = spark.table("workspace.silver.order_items")
first_seller = items.where("order_item_id = 1").select("order_id", "seller_id")
totals = items.groupBy("order_id").agg(
    F.sum("price").alias("total_price"),
    F.sum("freight_value").alias("total_freight"),
    F.count("*").alias("n_items"),
)
# Late = delivered on a later DAY than promised (the promise is a date, not a time).
delivered = orders.join(first_seller, "order_id").join(totals, "order_id").select(
    "order_id",
    "seller_id",
    "created_at",
    "delivered_at",
    "estimated_delivery_at",  # for when the label is decided (step 6); not in the spine
    F.datediff("estimated_delivery_at", "created_at").alias("promised_days"),
    "total_price",
    "total_freight",
    "n_items",
    (F.to_date("delivered_at") > F.to_date("estimated_delivery_at")).cast("int").alias("late"),
)
delivered = spark.createDataFrame(delivered.collect(), delivered.schema)  # fixed once (Drill 2 lesson)
spine = delivered.drop("delivered_at", "estimated_delivery_at")  # what is known at purchase
n, late = delivered.count(), delivered.where("late = 1").count()
print(f"spine: {n:,} delivered orders, {late:,} late ({late / n:.1%}); "
      f"before {CUTOFF}: {spine.where(F.col('created_at') < CUTOFF).count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The feature table — the seller's record, one row each time it changes
# MAGIC
# MAGIC A row says: *from `feature_ts` on, this seller had completed N deliveries, L of them late*. The
# MAGIC seller "as of time t" is the newest row with `feature_ts <= t`. The primary key's `TIMESERIES`
# MAGIC marks `feature_ts` as the time column — what makes it a point-in-time feature table.

# COMMAND ----------

per_moment = delivered.groupBy("seller_id", F.col("delivered_at").alias("feature_ts")).agg(
    F.count("*").alias("n"), F.sum("late").alias("l")
)
running = Window.partitionBy("seller_id").orderBy("feature_ts").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)
features = per_moment.select(
    "seller_id",
    "feature_ts",
    F.sum("n").over(running).alias("seller_deliveries"),
    F.sum("l").over(running).alias("seller_late"),
).withColumn("seller_late_rate", F.col("seller_late") / F.col("seller_deliveries"))
features.createOrReplaceTempView("seller_features_new")

spark.sql(
    f"""CREATE OR REPLACE TABLE {FEATURES} (
        seller_id STRING NOT NULL,
        feature_ts TIMESTAMP NOT NULL,
        seller_deliveries BIGINT,
        seller_late BIGINT,
        seller_late_rate DOUBLE,
        CONSTRAINT seller_delivery_features_pk PRIMARY KEY (seller_id, feature_ts TIMESERIES)
    ) COMMENT 'Seller delivery record as of feature_ts: cumulative deliveries completed, late ones.'"""
)
spark.sql(f"INSERT INTO {FEATURES} SELECT * FROM seller_features_new")
print(f"{FEATURES}: {spark.table(FEATURES).count():,} rows, "
      f"{spark.table(FEATURES).select('seller_id').distinct().count():,} sellers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The as-of join by hand — the answer key

# COMMAND ----------

f = spark.table(FEATURES).alias("f")
s = spine.alias("s")
as_of = (F.col("s.seller_id") == F.col("f.seller_id")) & (F.col("f.feature_ts") <= F.col("s.created_at"))
candidates = s.join(f, as_of, "left")
newest = Window.partitionBy("s.order_id").orderBy(F.col("f.feature_ts").desc_nulls_last())
by_hand = (
    candidates.withColumn("_rn", F.row_number().over(newest))
    .where("_rn = 1")
    .select("s.*", *[F.col(f"f.{c}") for c in SELLER_FEATURES])
)
by_hand = spark.createDataFrame(by_hand.collect(), by_hand.schema)
print(f"by hand: {by_hand.count():,} rows; orders whose seller had no completed delivery yet: "
      f"{by_hand.where('seller_deliveries IS NULL').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The same join by Feature Engineering — must equal step 3

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()
lookups = [
    FeatureLookup(
        table_name=FEATURES,
        lookup_key="seller_id",
        timestamp_lookup_key="created_at",  # the as-of: newest feature row with feature_ts <= created_at
        feature_names=SELLER_FEATURES,
    )
]
by_client = fe.create_training_set(df=spine, feature_lookups=lookups, label="late").load_df()

compared = by_hand.alias("h").join(by_client.alias("c"), "order_id", "full_outer")
different = compared.where(
    " OR ".join(f"NOT (h.{c} <=> c.{c})" for c in SELLER_FEATURES)
    + " OR h.order_id IS NULL OR c.order_id IS NULL"
).count()
print(f"by client: {by_client.count():,} rows; rows different from by hand: {different}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The leaky join — the seller over all time

# COMMAND ----------

all_time = delivered.groupBy("seller_id").agg(
    F.count("*").alias("seller_deliveries"), F.sum("late").alias("seller_late")
).withColumn("seller_late_rate", F.col("seller_late") / F.col("seller_deliveries"))
leaky = spine.join(all_time, "seller_id", "left")
# How much of the future it saw: orders whose all-time count includes deliveries after the order.
saw_future = by_hand.alias("h").join(leaky.alias("k"), "order_id").where(
    "k.seller_deliveries > coalesce(h.seller_deliveries, 0)"
).count()
print(f"leaky: {leaky.count():,} rows; orders whose feature counted deliveries from their future: "
      f"{saw_future:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Which orders may train — the label has a time too (Session 34)
# MAGIC
# MAGIC Steps 3 and 4 fixed every *feature* as of the order. The *label* — late or not — also becomes known at
# MAGIC a moment. A yes/no is decided earlier than a number of days: **on time** is known when the parcel
# MAGIC arrives, **late** the moment the promised day ends (a parcel still travelling then is late whenever
# MAGIC it arrives). Four rules, one test set (every order placed from the cutoff on), point-in-time
# MAGIC features for all:
# MAGIC
# MAGIC | rule | may train on | its flaw |
# MAGIC |---|---|---|
# MAGIC | placed | placed before the cutoff (S12b until S34) | labels not yet decided at the cutoff |
# MAGIC | delivered | arrived before the cutoff (S33's rule) | newest orders keep fast parcels only |
# MAGIC | decided | late-or-not decided before the cutoff | late shows only after the promise passes |
# MAGIC | matured | placed W days before the cutoff, decided by it | — |
# MAGIC
# MAGIC "delivered" also drops a late parcel still travelling, though its label is already known. "decided"
# MAGIC fixes that, but an order placed last week can only be decided on time so far (it arrived fast), so
# MAGIC its newest weeks still lean on time. W = the 99th percentile of days from order to decision, read
# MAGIC from orders settled long before the cutoff.
# MAGIC
# MAGIC Read per rule: rows; how many were still on the road at the cutoff, and how many of those labels
# MAGIC were not even decided yet; the late share of its last 30 days before the cutoff beside the truth for
# MAGIC those days; test AUC. A shuffled refit of the matured model gives the noise floor.

# COMMAND ----------

cut = pd.Timestamp(CUTOFF)
label_times = delivered.select("order_id", "delivered_at", "estimated_delivery_at").toPandas()


def training_frame(df):
    """A Spark training set as pandas, with each order's label times, sorted by order_id. Spark returns
    rows in no fixed order, and early stopping holds out 10% of the rows it is given by position: same
    seed + another order = another model (S33). Sorted, a re-run trains the same model."""
    return df.toPandas().merge(label_times, on="order_id").sort_values("order_id", ignore_index=True)


def as_float_inputs(X):
    """Only the model's inputs, by name, as float64 — whatever the caller passes.

    The first run's skew (incidents.md, 2026-10-06, Session 12b): in the notebook `toPandas()`
    turned a BIGINT with NULLs (a seller with no delivery yet) into float64 NaN; inside
    `score_batch`'s Spark UDF the same column arrived as pandas' nullable Int64 with pd.NA, which
    scikit-learn refuses. One conversion inside the model makes both paths the same.
    """
    return X[MODEL_INPUTS].astype("float64")


def model():
    pick = FunctionTransformer(as_float_inputs)
    return Pipeline([("pick", pick), ("gb", HistGradientBoostingClassifier(max_iter=200, random_state=0))])


def auc_on(m, df):
    return roc_auc_score(df.late, m.predict_proba(df)[:, 1])


p = training_frame(by_client)
created, arrived = p["created_at"], p["delivered_at"]
decided = late_label_known_at(arrived, p["estimated_delivery_at"])
W = maturity_window(created, decided, (decided - created) / pd.Timedelta(days=1), CUTOFF)
masks = {
    "placed": training_mask(created, decided, CUTOFF, "placed"),
    "delivered": training_mask(created, arrived, CUTOFF, "delivered"),  # label time = arrival
    "decided": training_mask(created, decided, CUTOFF, "delivered"),  # label time = decision
    "matured": training_mask(created, decided, CUTOFF, "matured", W),
}
test = p[created >= cut]
edge = (created >= cut - pd.Timedelta(days=30)) & (created < cut)

me = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{me}/ledgerline-late-delivery")
rows, models = [], {}
for rule, m in masks.items():
    train = p[m]
    with mlflow.start_run(run_name=f"label_{rule}"):
        fitted = model().fit(train, train.late)
        auc = auc_on(fitted, test)
        mlflow.log_params({"features": "point_in_time", "label_rule": rule, "cutoff": CUTOFF,
                           "window_days": W, "inputs": ",".join(MODEL_INPUTS), "rows_sorted": True})
        mlflow.log_metrics({"auc_test": auc, "auc_train": auc_on(fitted, train),
                            "rows_train": len(train), "rows_test": len(test)})
    models[rule] = fitted
    rows.append({
        "rule": rule,
        "train_rows": int(m.sum()),
        "on_the_road_at_cutoff": int((m & (arrived >= cut)).sum()),
        "undecided_at_cutoff": int((m & (decided >= cut)).sum()),
        "late_share": round(float(train.late.mean()), 4),
        "last_30_days_rows": int((m & edge).sum()),
        "last_30_days_late_share": round(float(p.loc[m & edge, "late"].mean()), 4),
        "test_auc": round(auc, 4),
    })
rows.append({"rule": "truth (all arrived)", "last_30_days_rows": int(edge.sum()),
             "last_30_days_late_share": round(float(p.loc[edge, "late"].mean()), 4)})
print(f"W = {W} days (99th percentile of days from order to decision, orders settled long before the "
      f"cutoff); test orders: {len(test):,}, late {test.late.mean():.4f}; S12b's test AUC was 0.5977")
print(pd.DataFrame(rows).to_string(index=False))

# The noise floor: the same matured rows, shuffled. Sorted, a refit must agree exactly.
matured = p[masks["matured"]]
shuffled = matured.sample(frac=1, random_state=1)
base = models["matured"].predict_proba(test)[:, 1]
for name, rows_in in (("sorted, refit", matured), ("shuffled", shuffled)):
    refit = model().fit(rows_in, rows_in.late)
    prob = refit.predict_proba(test)[:, 1]
    print(f"{name:14} test AUC {auc_on(refit, test):.4f}; orders whose probability differs from the "
          f"matured model: {int((np.abs(prob - base) > 1e-12).sum()):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6b. The feature leak — honest vs leaky features, both on the matured rows
# MAGIC
# MAGIC Three numbers: the honest model on honest test features; the leaky model on leaky test features
# MAGIC (what its notebook would report); the leaky model on honest test features (what it does in use —
# MAGIC at prediction time only the past exists). Until Session 34 these trained on the "placed" rows.

# COMMAND ----------

leaky_p = training_frame(leaky)
leaky_train = leaky_p[leaky_p.order_id.isin(matured.order_id)]
leaky_test = leaky_p[leaky_p.created_at >= cut]
assert len(leaky_train) == len(matured) and len(leaky_test) == len(test), "leaky rows differ from honest"
with mlflow.start_run(run_name="leaky_all_time_matured"):
    leaky_model = model().fit(leaky_train, leaky_train.late)
    results = {
        "point_in_time (matured): test AUC": auc_on(models["matured"], test),
        "leaky_all_time (matured): test AUC": auc_on(leaky_model, leaky_test),
        "leaky model, honest test features (in use)": auc_on(leaky_model, test),
        "leaky_all_time (matured): train AUC": auc_on(leaky_model, leaky_train),
    }
    mlflow.log_params({"features": "leaky_all_time", "label_rule": "matured", "cutoff": CUTOFF,
                       "window_days": W, "inputs": ",".join(MODEL_INPUTS), "rows_sorted": True})
    mlflow.log_metrics({"auc_test": results["leaky_all_time (matured): test AUC"],
                        "auc_train": results["leaky_all_time (matured): train AUC"],
                        "auc_test_on_point_in_time_features":
                            results["leaky model, honest test features (in use)"],
                        "rows_train": len(leaky_train), "rows_test": len(leaky_test)})
for k, v in results.items():
    print(f"{k:45} {v:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Register the matured, point-in-time model in Unity Catalog
# MAGIC
# MAGIC Logged with Feature Engineering, the model remembers its feature lookups: given only the spine
# MAGIC (ids + time + order columns) it looks the seller up **as of each order's time** itself. Scoring
# MAGIC through it is `databricks/ml/score_champion`, in a session of its own.

# COMMAND ----------

from mlflow import MlflowClient

mlflow.set_registry_uri("databricks-uc")
train_ids = spark.createDataFrame(matured[["order_id"]])
training_set = fe.create_training_set(df=spine.join(train_ids, "order_id"), feature_lookups=lookups,
                                      label="late")
log_args = {"model": models["matured"], "flavor": mlflow.sklearn, "training_set": training_set,
            "registered_model_name": MODEL}
with mlflow.start_run(run_name="point_in_time_matured_registered"):
    mlflow.log_params({"label_rule": "matured", "window_days": W, "rows_train": len(matured)})
    try:
        fe.log_model(artifact_path="model", **log_args)
    except TypeError as e:  # newer MLflow names it `name`; print which one this workspace has
        print(f"artifact_path refused ({e}); using name=")
        fe.log_model(name="model", **log_args)
print(f"mlflow {mlflow.__version__}")
client = MlflowClient(registry_uri="databricks-uc")
version = max(int(v.version) for v in client.search_model_versions(f"name = '{MODEL}'"))
client.set_registered_model_alias(MODEL, "champion", version)
print(f"{MODEL}: version {version} = @champion (rule matured, W = {W} days, {len(matured):,} rows)")

# Scoring is a separate notebook (`score_champion`), in a fresh session — the way production scores,
# and the first run showed why: in this session score_batch kept answering like an older version.
# What it is compared with: this notebook's own predictions on the test orders, saved here.
TEST = f"{ML}.late_delivery_test_orders"
step6 = test.assign(step6_prediction=models["matured"].predict(test))
saved = spark.createDataFrame(step6[["order_id", "step6_prediction"]]).join(
    spine.where(F.col("created_at") >= CUTOFF), "order_id"
)
saved.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TEST)
print(f"{TEST}: {spark.table(TEST).count():,} test orders with step 6's prediction "
      f"({int(step6.step6_prediction.sum()):,} predicted late); champion = version {version}")