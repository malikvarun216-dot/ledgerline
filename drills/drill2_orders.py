# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Drill 2 — Silver orders attacked: history backwards, and a checkpoint reset
# MAGIC
# MAGIC Silver orders (Session 10) is an accumulating snapshot: one row per order, one timestamp column per
# MAGIC lifecycle step, each filled once. The design claims **arrival order does not matter** — `shipped`
# MAGIC before `created` lands the same as after — which is why it has no `seq` and no guard. This notebook
# MAGIC tests the claim the hard way, with the production code (`_merge_orders`) on scratch tables
# MAGIC `workspace.silver.drill2_o*`; production Bronze and Silver are only read.
# MAGIC
# MAGIC | Part | Attack | Expected |
# MAGIC |---|---|---|
# MAGIC | B | Bronze's 9 batches applied **newest first** | = production; counts as predicted from Bronze |
# MAGIC | R | restarted with no checkpoint, same app id | MERGEs change nothing; log line silently skipped |
# MAGIC | G | the reset guard | refused before a stream starts |
# MAGIC
# MAGIC The same reset trap with new data waiting, and its effect on the lag alarm, is in `drill2_cdc`
# MAGIC (the same code shape: MERGEs safe to repeat, `txnVersion` on the log line only).

# COMMAND ----------

# MAGIC %run ../databricks/silver/_merge_orders

# COMMAND ----------

import uuid

RUN = uuid.uuid4().hex[:8]
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.silver.checkpoints")


def scratch(part):
    t = {k: f"{SILVER}.drill2_{part}_{k}" for k in ("orders", "items", "log")}
    for name in t.values():
        spark.sql(f"DROP TABLE IF EXISTS {name}")
    t["app_id"] = f"ledgerline.drill2.{RUN}.{part}"
    t["checkpoint"] = f"{CHECKPOINTS}/drill2/{RUN}/{part}"
    return t


def lines(t):
    return spark.table(t["log"]).orderBy("applied_at", "batch_id").collect()


def commits(table, operation):
    history = spark.sql(f"DESCRIBE HISTORY {table}")
    return history.where(f"operation = '{operation}'").orderBy("version").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## B — Bronze's history applied newest first
# MAGIC
# MAGIC **Prediction, from Bronze alone** (no MERGE): for each order and step, the batch that brings that step
# MAGIC first is the **highest** Bronze batch holding it (newest first). An order is inserted by the first of
# MAGIC those batches and updated by each other one; a batch holding only steps already brought (a re-send)
# MAGIC does nothing. The same formula in Bronze's own order (lowest batch first) must give production's
# MAGIC numbers — checked first, so the backwards prediction is not trusted blind.

# COMMAND ----------

bronze = spark.table(BRONZE_ORDERS)
batches = [r._batch_id for r in bronze.select("_batch_id").distinct().orderBy("_batch_id").collect()]
backwards = list(reversed(batches))
print(f"Bronze batches {batches}; applied as {backwards}")

step_batches = bronze.groupBy("order_id", "event_type").agg(
    F.min("_batch_id").alias("fwd"), F.max("_batch_id").alias("bwd")
)


def predict(column, first):
    """{bronze batch: (inserted, updated)} when batches are applied in the order `first` picks."""
    brings = step_batches.select("order_id", F.col(column).alias("b")).distinct()
    w = Window.partitionBy("order_id")
    starts = brings.withColumn("_first", first(F.col("b")).over(w))
    return {
        r.b: (r.ins, r.upd)
        for r in starts.groupBy("b").agg(
            F.count_if(F.col("b") == F.col("_first")).alias("ins"),
            F.count_if(F.col("b") != F.col("_first")).alias("upd"),
        ).collect()
    }


forward = predict("fwd", F.min)
production = {r.bronze_batch_min: (r.inserted, r.updated) for r in spark.table(ORDERS_LOG).collect()}
print(f"formula in Bronze's order: {dict(sorted(forward.items()))}\nproduction's log:          "
      f"{dict(sorted(production.items()))}")
assert {b: forward.get(b, (0, 0)) for b in production} == production
assert sum(u for _, u in forward.values()) == 18_587

predicted = predict("bwd", F.max)
created = bronze.where("event_type = 'created'").groupBy("order_id").agg(
    F.max("_batch_id").alias("b"),
    F.max(F.when(F.col("items").isNull(), F.lit(0)).otherwise(F.size("items"))).alias("units"),
)
predicted_items = {r.b: r.units for r in created.groupBy("b").agg(F.sum("units").alias("units")).collect()}
print(f"predicted backwards: {dict(sorted(predicted.items(), reverse=True))}; items {predicted_items}")

# COMMAND ----------

B = scratch("ob")
create_tables(B["orders"], B["items"], B["log"])
apply_b = make_batch_writer(
    app_id=B["app_id"], orders_table=B["orders"], items_table=B["items"], log_table=B["log"]
)
for k, b in enumerate(backwards):
    apply_b(bronze.where(F.col("_batch_id") == b), k)

got = lines(B)
for k, r in enumerate(got):
    print(f"applied {k} <- Bronze batch {backwards[k]}: rows {r.bronze_rows:,} "
          f"+{r.inserted:,} ~{r.updated:,} items {r.items_inserted:,}  "
          f"predicted {predicted.get(backwards[k], (0, 0))} items {predicted_items.get(backwards[k], 0):,}")
assert all((r.inserted, r.updated) == predicted.get(backwards[k], (0, 0)) for k, r in enumerate(got))
assert all(r.items_inserted == predicted_items.get(backwards[k], 0) for k, r in enumerate(got))
assert sum(r.inserted for r in got) == 99_441
print(f"updates backwards {sum(r.updated for r in got):,} vs forwards 18,587")

# COMMAND ----------

# MAGIC %md
# MAGIC **The claim itself:** backwards Silver equals production on every business column of every order and
# MAGIC item, both directions. Lineage columns may differ — they record which copy of a re-sent event was
# MAGIC applied, and newest first, the later copy wins.

# COMMAND ----------


def both_ways(a, b):
    return a.exceptAll(b).count(), b.exceptAll(a).count()


orders_diff = both_ways(
    spark.table(B["orders"]).select(*ORDER_FIELDS), spark.table(ORDERS).select(*ORDER_FIELDS)
)
items_diff = both_ways(spark.table(B["items"]).select(*ITEM_FIELDS), spark.table(ITEMS).select(*ITEM_FIELDS))
coords = ["order_id", "order_item_id", "_event_id", *COORDINATES]
moved = spark.table(B["items"]).select(*coords).exceptAll(spark.table(ITEMS).select(*coords)).count()
resent_created = bronze.where("event_type = 'created'").groupBy("event_id").agg(
    F.count_distinct("_batch_id").alias("n"),
    F.max(F.when(F.col("items").isNull(), F.lit(0)).otherwise(F.size("items"))).alias("units"),
).where("n > 1")
resent_units = resent_created.agg(F.sum("units")).first()[0] or 0
print(f"orders vs production {orders_diff}; items vs production {items_diff}")
print(f"items taken from another copy: {moved} "
      f"(units on `created` events re-sent in a later batch: {resent_units})")
assert orders_diff == (0, 0) and items_diff == (0, 0)
assert moved <= resent_units
print("HELD: newest first, Silver orders is identical to production — arrival order does not matter")

# COMMAND ----------

# MAGIC %md
# MAGIC ## R — the stream restarted with no checkpoint, same app id. **Bug first.**
# MAGIC
# MAGIC The backwards tables' log remembers batches 0 to 8 for its app id. A stream with a new checkpoint
# MAGIC reads all of Bronze as batch 0: the MERGEs find nothing new (safe to repeat), and Delta skips the log
# MAGIC line (batch 0 ≤ 8). Nothing is lost here because nothing new was waiting — `drill2_cdc` R1 is the
# MAGIC same trap with new rows waiting.

# COMMAND ----------

log_writes = len(commits(B["log"], "WRITE"))
merges_before = len(commits(B["orders"], "MERGE"))
run_silver_orders(
    checkpoint=B["checkpoint"], app_id=B["app_id"], orders_table=B["orders"], items_table=B["items"],
    log_table=B["log"], guard_reset=False,
)
new_merges = commits(B["orders"], "MERGE")[merges_before:]
METRICS = ("numSourceRows", "numTargetRowsInserted", "numTargetRowsUpdated")
metrics = [{k: int(m.operationMetrics.get(k) or 0) for k in METRICS} for m in new_merges]
print(f"orders MERGEs of the restarted stream: {metrics}; log lines {len(lines(B))}, "
      f"log WRITE commits {log_writes} -> {len(commits(B['log'], 'WRITE'))}")
assert len(new_merges) >= 1, "the restarted stream applied no batch at all"
assert all(m["numTargetRowsInserted"] == 0 and m["numTargetRowsUpdated"] == 0 for m in metrics)
assert len(lines(B)) == len(backwards) and len(commits(B["log"], "WRITE")) == log_writes
print("BUG PRESENT: a whole pass over Bronze ran and left no line in the log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## G — the guard refuses the same restart before any stream starts

# COMMAND ----------

fresh = f"{B['checkpoint']}-again"
try:
    run_silver_orders(
        checkpoint=fresh, app_id=B["app_id"], orders_table=B["orders"], items_table=B["items"],
        log_table=B["log"],
    )
    refused = None
except UnsafeStreamReset as e:
    refused = str(e)
print((refused or "NOT refused")[:300])
assert refused and not checkpoint_has_history(fresh)
assert checkpoint_has_history(f"{CHECKPOINTS}/orders/v1"), "production's daily run would be refused"
print("GUARD SEEN: refused before readStream; production's checkpoint passes")

# COMMAND ----------

HISTORY_COLUMNS = ["version", "timestamp", "operation", "operationMetrics"]
display(spark.sql(f"DESCRIBE HISTORY {B['orders']}").select(*HISTORY_COLUMNS))