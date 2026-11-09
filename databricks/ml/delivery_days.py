# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Delivery days — how long will this order take? (Session 33)
# MAGIC
# MAGIC **The question:** at the moment an order is placed, how many calendar days until it arrives? Beside
# MAGIC S12b's yes/no ("will it be late?"), this is the number a customer is actually shown.
# MAGIC
# MAGIC **The trap this notebook is about:** S12b's trap was a *feature* from the future. This one is a
# MAGIC *label* from the future. A delivery time exists only once the parcel arrives. A model trained at
# MAGIC 2018-01-01 on "every order placed before that day" learns from parcels that arrived in January —
# MAGIC labels nobody had on 1 January. Keep only labels known by then and the newest orders keep only their
# MAGIC fast deliveries (the slow ones are still on the road). The fix is a **maturity window**: train on
# MAGIC orders old enough that almost all of them have arrived (decisions.md, Session 33).
# MAGIC
# MAGIC | step | what | read against |
# MAGIC |---|---|---|
# MAGIC | 0 | Gold and S12b's feature table can be read | — |
# MAGIC | 1 | spine from Gold: delivered orders; customer and seller **as at purchase** | S12b's 96,470 |
# MAGIC | 2 | a second seller feature table: average delivery days, keyed `(seller_id, feature_ts)` | — |
# MAGIC | 3 | training set by Feature Engineering: two tables, looked up as of the purchase | spine rows |
# MAGIC | 4 | three label rules: rows, labels from after the cutoff, its last month's speed | the truth |
# MAGIC | 5 | a median model per rule + three baselines, one test set; MLflow logs each | MAE in days |
# MAGIC | 6 | a promise model (a quantile) beside Olist's own promise | late share and days promised |
# MAGIC | 7 | register the matured-rule model in Unity Catalog as `@champion`; save its test predictions | — |
# MAGIC
# MAGIC Scoring through the registered model is `databricks/ml/score_champion`, in a session of its own
# MAGIC (widgets `model` = `workspace.ml.delivery_days`,
# MAGIC `test_table` = `workspace.ml.delivery_days_test_orders`, `expected_column` = `trained_prediction`).
# MAGIC
# MAGIC Reads `workspace.gold` (dbt's `databricks` target) and S12b's `workspace.ml.seller_delivery_features`;
# MAGIC writes only to `workspace.ml`. Safe to "Run all" again: tables are rebuilt, each run adds MLflow runs
# MAGIC and one model version.

# COMMAND ----------

# MAGIC %pip install --quiet databricks-feature-engineering scikit-learn

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_delivery_days

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from pyspark.sql import Window
from pyspark.sql import functions as F
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer

ML = "workspace.ml"
GOLD = "workspace.gold"
LATE_FEATURES = f"{ML}.seller_delivery_features"  # S12b's, reused unchanged
DAYS_FEATURES = f"{ML}.seller_delivery_days_features"  # this notebook's
MODEL = f"{ML}.delivery_days"
TEST = f"{ML}.delivery_days_test_orders"
CUTOFF = "2018-01-01"  # train on what was known before; test on orders placed after
S12B_SPINE = 96_470  # S12b's delivered orders, read from Silver (progress.md, Session 12b)
SPINE_COLUMNS = ["order_id", "seller_id", "created_at", *ORDER_NUMERIC, *STATE_COLUMNS, "actual_days"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ML} COMMENT 'Session 12b: features and models'")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Can this notebook read what it needs?
# MAGIC
# MAGIC Gold on Databricks was built by `ledgerline-ci` (Session 28) and has only been read as that service
# MAGIC principal so far. If this cell is refused, nothing below can run: the fix is a grant, not a copy.

# COMMAND ----------

for name in (f"{GOLD}.delivery_promise", f"{GOLD}.order_items_at_sale", LATE_FEATURES):
    print(f"{name}: {spark.table(name).count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The spine — one row per delivered order, as it was at the moment it was placed
# MAGIC
# MAGIC `delivery_promise` gives the promised and actual days (calendar days — the promise is a date) and
# MAGIC the customer's state **when they ordered**; `order_items_at_sale` gives the first item's seller and
# MAGIC the seller's state at the sale. Silver could not: it holds where a person lives *now*.

# COMMAND ----------

promise = spark.table(f"{GOLD}.delivery_promise").where("order_status = 'delivered'")
items = spark.table(f"{GOLD}.order_items_at_sale")
first_item = items.where("order_item_id = 1").select("order_id", "seller_id", "seller_state")
totals = items.groupBy("order_id").agg(
    F.sum("price").alias("total_price"),
    F.sum("freight_value").alias("total_freight"),
    F.count("*").alias("n_items"),
)
delivered = promise.join(first_item, "order_id").join(totals, "order_id").select(
    "order_id",
    "seller_id",
    F.col("purchased_at").cast("timestamp").alias("created_at"),
    F.col("delivered_at").cast("timestamp").alias("delivered_at"),
    F.col("promised_days").cast("int").alias("promised_days"),
    F.col("actual_days").cast("int").alias("actual_days"),
    # Gold's money is DECIMAL(12,2); an MLflow signature has no decimal type, and Unity Catalog will not
    # register a model without a signature (S33's first run). Same values, DOUBLE.
    F.col("total_price").cast("double").alias("total_price"),
    F.col("total_freight").cast("double").alias("total_freight"),
    "n_items",
    F.month("purchased_at").alias("purchase_month"),
    F.dayofweek("purchased_at").alias("purchase_weekday"),
    "customer_state",
    "seller_state",
)
delivered = spark.createDataFrame(delivered.collect(), delivered.schema)  # fixed once (Drill 2 lesson)
spine = delivered.select(*SPINE_COLUMNS)

n = delivered.count()
s = delivered.selectExpr(
    "percentile(actual_days, array(0.5, 0.9, 0.99)) AS q",
    "max(actual_days) AS longest",
    "avg(actual_days) AS mean_days",
    "count_if(customer_state IS NULL) AS no_customer_state",
    "count_if(seller_state IS NULL) AS no_seller_state",
    "avg(CAST(customer_state = seller_state AS INT)) AS same_state",
    "count_if(created_at < '2018-01-01') AS before_cutoff",
).first()
print(f"spine: {n:,} delivered orders (S12b from Silver: {S12B_SPINE:,}; difference {n - S12B_SPINE:+,})")
print(f"actual days: median {s.q[0]:.0f}, p90 {s.q[1]:.0f}, p99 {s.q[2]:.0f}, longest {s.longest}, "
      f"mean {s.mean_days:.2f} — the mean sits above the median: a long tail")
print(f"no customer state: {s.no_customer_state:,}; no seller state: {s.no_seller_state:,}; "
      f"same state: {s.same_state:.1%}; placed before {CUTOFF}: {s.before_cutoff:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. A second seller feature table — the seller's average delivery days, as of each delivery
# MAGIC
# MAGIC Same shape as S12b's: a row says *from `feature_ts` on, this seller's completed deliveries took D
# MAGIC days on average*. S12b's table (deliveries, late rate) is reused as it is — one feature table, two
# MAGIC models. Worth knowing: "completed" deliveries at time t are the fast ones among recent orders, so
# MAGIC this feature has the same lean as the labels in step 4.

# COMMAND ----------

per_moment = delivered.groupBy("seller_id", F.col("delivered_at").alias("feature_ts")).agg(
    F.count("*").alias("n"), F.sum("actual_days").alias("d")
)
running = Window.partitionBy("seller_id").orderBy("feature_ts").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)
days_features = per_moment.select(
    "seller_id",
    "feature_ts",
    F.sum("d").over(running).alias("seller_days_total"),
    (F.sum("d").over(running) / F.sum("n").over(running)).alias("seller_avg_days"),
)
days_features.createOrReplaceTempView("seller_days_features_new")

spark.sql(
    f"""CREATE OR REPLACE TABLE {DAYS_FEATURES} (
        seller_id STRING NOT NULL,
        feature_ts TIMESTAMP NOT NULL,
        seller_days_total BIGINT,
        seller_avg_days DOUBLE,
        CONSTRAINT seller_delivery_days_features_pk PRIMARY KEY (seller_id, feature_ts TIMESERIES)
    ) COMMENT 'Seller delivery days as of feature_ts: total and average days of completed deliveries.'"""
)
spark.sql(f"INSERT INTO {DAYS_FEATURES} SELECT * FROM seller_days_features_new")
print(f"{DAYS_FEATURES}: {spark.table(DAYS_FEATURES).count():,} rows, "
      f"{spark.table(DAYS_FEATURES).select('seller_id').distinct().count():,} sellers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The training set — Feature Engineering looks both tables up as of each purchase

# COMMAND ----------

fe = FeatureEngineeringClient()
lookups = [
    FeatureLookup(table_name=LATE_FEATURES, lookup_key="seller_id", timestamp_lookup_key="created_at",
                  feature_names=["seller_deliveries", "seller_late_rate"]),
    FeatureLookup(table_name=DAYS_FEATURES, lookup_key="seller_id", timestamp_lookup_key="created_at",
                  feature_names=["seller_avg_days"]),
]
full = fe.create_training_set(df=spine, feature_lookups=lookups, label="actual_days").load_df()
p = full.toPandas().merge(delivered.select("order_id", "delivered_at").toPandas(), on="order_id")
print(f"training set: {len(p):,} rows (spine {n:,}); "
      f"no seller history yet: {int(p.seller_deliveries.isna().sum()):,}; "
      f"no average days yet: {int(p.seller_avg_days.isna().sum()):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The three label rules at the cutoff
# MAGIC
# MAGIC For each rule: how many orders it may train on, how many of those labels only existed **after**
# MAGIC the cutoff, and how fast its last 30 days before the cutoff look — beside the truth for those days
# MAGIC (every order placed then, known now that all have arrived).

# COMMAND ----------

cut = pd.Timestamp(CUTOFF)
created, arrived = p["created_at"], p["delivered_at"]
W = maturity_window(created, arrived, p["actual_days"], CUTOFF)
masks = {rule: training_mask(created, arrived, CUTOFF, rule, W) for rule in RULES}
test = p[created >= cut]
edge = (created >= cut - pd.Timedelta(days=30)) & (created < cut)

rows = []
for rule, m in masks.items():
    rows.append({
        "rule": rule,
        "train_rows": int(m.sum()),
        "labels_from_after_cutoff": int((m & (arrived >= cut)).sum()),
        "last_30_days_rows": int((m & edge).sum()),
        "last_30_days_mean_days": round(float(p.loc[m & edge, "actual_days"].mean()), 2),
    })
rows.append({"rule": "truth (all arrived)", "train_rows": None, "labels_from_after_cutoff": None,
             "last_30_days_rows": int(edge.sum()),
             "last_30_days_mean_days": round(float(p.loc[edge, "actual_days"].mean()), 2)})
print(f"maturity window W = {W} days (99th percentile of delivery days, orders settled long before the "
      f"cutoff); test orders: {len(test):,}")
print(pd.DataFrame(rows).to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. A median model per rule, three baselines — one test set
# MAGIC
# MAGIC The loss is absolute error, so each model predicts the **median** (half the parcels arrive by then).
# MAGIC Baselines: the overall median, the median of the route (seller state → customer state), and
# MAGIC Olist's own promise read as if it were a forecast (it is not: it is padded on purpose).

# COMMAND ----------


def eta_model():
    return Pipeline([
        ("inputs", FunctionTransformer(model_inputs)),
        ("gb", HistGradientBoostingRegressor(loss="absolute_error", categorical_features=CATEGORICAL,
                                             max_iter=300, random_state=0)),
    ])


me = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{me}/ledgerline-delivery-days")
results, models = [], {}
for rule, m in masks.items():
    train = p[m]
    with mlflow.start_run(run_name=f"eta_{rule}"):
        model = eta_model().fit(train, train["actual_days"])
        e = errors(test["actual_days"], model.predict(test))
        e_train = errors(train["actual_days"], model.predict(train))
        mlflow.log_params({"label_rule": rule, "cutoff": CUTOFF, "window_days": W, "loss": "absolute_error",
                           "inputs": ",".join(MODEL_INPUTS)})
        mlflow.log_metrics({**{f"test_{k}": v for k, v in e.items()}, "train_mae": e_train["mae"],
                            "rows_train": len(train), "rows_test": len(test)})
    models[rule] = model
    results.append({"forecast": f"model, rule {rule}", **e, "train_mae": e_train["mae"]})

matured = p[masks["matured"]]
overall = np.full(len(test), float(matured["actual_days"].median()))
results.append({"forecast": "overall median (matured rows)", **errors(test["actual_days"], overall)})
results.append({"forecast": "route median (matured rows)",
                **errors(test["actual_days"], route_median(matured, test))})
results.append({"forecast": "Olist's promise, read as a forecast",
                **errors(test["actual_days"], test["promised_days"])})
print(pd.DataFrame(results).round(3).to_string(index=False))
print(f"test actual days: median {test.actual_days.median():.0f}, mean {test.actual_days.mean():.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The promise — a quantile model beside Olist's
# MAGIC
# MAGIC Olist promises late on a small share of orders. A promise is a high quantile, not the median: set
# MAGIC q = Olist's on-time share in the training rows, and compare like for like on the test orders — the
# MAGIC late share each promise gives, and how many days each one promises on average.

# COMMAND ----------

olist_train = promise_report(matured["actual_days"], matured["promised_days"])
q = round(1 - olist_train["late_rate"], 3)
with mlflow.start_run(run_name="promise_matured"):
    promise_model = Pipeline([
        ("inputs", FunctionTransformer(model_inputs)),
        ("gb", HistGradientBoostingRegressor(loss="quantile", quantile=q, categorical_features=CATEGORICAL,
                                             max_iter=300, random_state=0)),
    ]).fit(matured, matured["actual_days"])
    ours = promise_report(test["actual_days"], np.ceil(promise_model.predict(test)))
    olist = promise_report(test["actual_days"], test["promised_days"])
    mlflow.log_params({"label_rule": "matured", "loss": "quantile", "quantile": q, "window_days": W})
    mlflow.log_metrics({"test_late_rate": ours["late_rate"],
                        "test_mean_promised_days": ours["mean_promised_days"],
                        "olist_test_late_rate": olist["late_rate"],
                        "olist_test_mean_promised_days": olist["mean_promised_days"]})
print(f"q = {q} (Olist on time in training: {1 - olist_train['late_rate']:.3f}, "
      f"promising {olist_train['mean_promised_days']:.2f} days on average)")
for who, r in (("Olist's promise", olist), ("our promise", ours)):
    print(f"test, {who:16} late {r['late_rate']:.3%}, {r['mean_promised_days']:.2f} days promised")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Register the matured-rule model in Unity Catalog
# MAGIC
# MAGIC Logged with Feature Engineering, so the model carries its two lookups: given only the spine it finds
# MAGIC the seller as of each order's time itself. Its test predictions are saved for `score_champion`.

# COMMAND ----------

from mlflow import MlflowClient

mlflow.set_registry_uri("databricks-uc")
train_ids = spark.createDataFrame(matured[["order_id"]])
training_set = fe.create_training_set(df=spine.join(train_ids, "order_id"), feature_lookups=lookups,
                                      label="actual_days")
log_args = {"model": models["matured"], "flavor": mlflow.sklearn, "training_set": training_set,
            "registered_model_name": MODEL}
with mlflow.start_run(run_name="eta_matured_registered"):
    try:
        fe.log_model(artifact_path="model", **log_args)
    except TypeError as e:  # newer MLflow names it `name`
        print(f"artifact_path refused ({e}); using name=")
        fe.log_model(name="model", **log_args)
client = MlflowClient(registry_uri="databricks-uc")
version = max(int(v.version) for v in client.search_model_versions(f"name = '{MODEL}'"))
client.set_registered_model_alias(MODEL, "champion", version)

trained = test[["order_id"]].assign(trained_prediction=models["matured"].predict(test))
saved = spark.createDataFrame(trained).join(spine.where(F.col("created_at") >= CUTOFF), "order_id")
saved.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TEST)
print(f"{MODEL}: version {version} = @champion; {TEST}: {spark.table(TEST).count():,} test orders")