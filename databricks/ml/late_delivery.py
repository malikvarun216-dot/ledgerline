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
# MAGIC | 6 | train both, time split; MLflow logs both | AUC: honest vs leaky vs leaky-in-use |
# MAGIC | 7 | register the honest model in Unity Catalog, alias `champion`; score through it | step 6's AUC |
# MAGIC
# MAGIC Writes only to `workspace.ml`. Safe to "Run all" again: tables are rebuilt from Silver, each run
# MAGIC adds MLflow runs and one model version.

# COMMAND ----------

# MAGIC %pip install --quiet databricks-feature-engineering scikit-learn

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow
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
    F.datediff("estimated_delivery_at", "created_at").alias("promised_days"),
    "total_price",
    "total_freight",
    "n_items",
    (F.to_date("delivered_at") > F.to_date("estimated_delivery_at")).cast("int").alias("late"),
)
delivered = spark.createDataFrame(delivered.collect(), delivered.schema)  # fixed once (Drill 2 lesson)
spine = delivered.drop("delivered_at")
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
# MAGIC ## 6. Train both, time split; MLflow logs both
# MAGIC
# MAGIC Three numbers: the honest model on honest test features; the leaky model on leaky test features
# MAGIC (what its notebook would report); the leaky model on honest test features (what it does in use —
# MAGIC at prediction time only the past exists).

# COMMAND ----------


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


def split(df):
    p = df.toPandas()
    train, test = p[p.created_at < pd.Timestamp(CUTOFF)], p[p.created_at >= pd.Timestamp(CUTOFF)]
    return train, test


me = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{me}/ledgerline-late-delivery")
results = {}
honest_train, honest_test = split(by_client)
leaky_train, leaky_test = split(leaky)
runs = (("point_in_time", honest_train, honest_test), ("leaky_all_time", leaky_train, leaky_test))
for name, train, test in runs:
    with mlflow.start_run(run_name=name) as run:
        m = model().fit(train, train.late)
        auc_test = roc_auc_score(test.late, m.predict_proba(test)[:, 1])
        auc_train = roc_auc_score(train.late, m.predict_proba(train)[:, 1])
        mlflow.log_params({"features": name, "cutoff": CUTOFF, "inputs": ",".join(MODEL_INPUTS)})
        mlflow.log_metrics({"auc_test": auc_test, "auc_train": auc_train,
                            "rows_train": len(train), "rows_test": len(test)})
        if name == "leaky_all_time":
            in_use = roc_auc_score(honest_test.late, m.predict_proba(honest_test)[:, 1])
            mlflow.log_metric("auc_test_on_point_in_time_features", in_use)
            results["leaky model, honest test features (in use)"] = in_use
        results[f"{name}: test AUC"] = auc_test
        results[f"{name}: train AUC"] = auc_train
        results[f"{name}: run id"] = run.info.run_id
        if name == "point_in_time":
            honest_model = m
for k, v in results.items():
    print(f"{k:45} {v if isinstance(v, str) else round(v, 4)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Register the honest model in Unity Catalog, and score through it
# MAGIC
# MAGIC Logged with Feature Engineering, the model remembers its feature lookups: `score_batch` is given
# MAGIC only the spine (ids + time + order columns) and looks the seller up **as of each order's time**
# MAGIC itself. Its test AUC must equal step 6's.

# COMMAND ----------

from mlflow import MlflowClient

mlflow.set_registry_uri("databricks-uc")
train_spine = spine.where(F.col("created_at") < CUTOFF)
training_set = fe.create_training_set(df=train_spine, feature_lookups=lookups, label="late")
log_args = {"model": honest_model, "flavor": mlflow.sklearn, "training_set": training_set,
            "registered_model_name": MODEL}
with mlflow.start_run(run_name="point_in_time_registered"):
    try:
        fe.log_model(artifact_path="model", **log_args)
    except TypeError as e:  # newer MLflow names it `name`; print which one this workspace has
        print(f"artifact_path refused ({e}); using name=")
        fe.log_model(name="model", **log_args)
print(f"mlflow {mlflow.__version__}")
client = MlflowClient(registry_uri="databricks-uc")
version = max(int(v.version) for v in client.search_model_versions(f"name = '{MODEL}'"))
client.set_registered_model_alias(MODEL, "champion", version)
print(f"{MODEL}: version {version} = @champion")

test_spine = spine.where(F.col("created_at") >= CUTOFF)
scored = fe.score_batch(model_uri=f"models:/{MODEL}@champion", df=test_spine)
scored = scored.select("order_id", "prediction").toPandas()
mine = honest_test.assign(step6=honest_model.predict(honest_test))[["order_id", "step6"]]
both = mine.merge(scored, on="order_id", how="outer", indicator=True)
agree = (both.step6 == both.prediction).sum()
print(f"score_batch scored {len(scored):,} test orders; same 0/1 prediction as step 6's model: "
      f"{agree:,} of {len(both):,} (only on one side: {(both._merge != 'both').sum()})")