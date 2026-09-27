# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Drill 1 — checkpoint-reset trap, part 1 of 2: life 1
# MAGIC
# MAGIC **Run this BEFORE the Drill 1 produce** (two 10-order partial runs to `orders`).
# MAGIC
# MAGIC A stream has two memories, kept in two places:
# MAGIC
# MAGIC | Memory | Where | What it says |
# MAGIC |---|---|---|
# MAGIC | Spark's | the **checkpoint** folder | which batches ran, and which offsets each covered |
# MAGIC | Delta's | the **table's log** | "app `drill1.trap.v1` has written up to version N" |
# MAGIC
# MAGIC Life 1 is an ordinary, clean load of the whole topic into a scratch table, in 50,000-offset
# MAGIC batches. Afterwards Delta remembers app `drill1.trap.v1` at version 7 (batches 0 to 7).
# MAGIC Part 2 then deletes Spark's memory and keeps Delta's — see `learning.md`, C6, "the
# MAGIC checkpoint-deleted trap, with 25 messages". This is the same story at 394,090.

# COMMAND ----------

# MAGIC %run ./_drill1_trap_common

# COMMAND ----------

drop_scratch()
life1 = run_stream(**trap_args("drill1.trap.life1", CHECKPOINT_V1, APP_ID_V1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Life 1 landed every message once

# COMMAND ----------

LIFE1_END = topic_end(life1)
print(f"topic end at life 1 (broker): {LIFE1_END}  total {sum(LIFE1_END.values()):,}")
check_coordinates(TRAP_TABLE, TRAP_QUARANTINE, LIFE1_END)
check_batches_not_doubled(TRAP_TABLE)

offsets, commits = batch_files(CHECKPOINT_V1, "offsets"), batch_files(CHECKPOINT_V1, "commits")
print(f"checkpoint: offsets {offsets}, commits {commits}")
print(f"Delta now remembers app {APP_ID_V1!r} at version {commits[-1]}")
assert offsets == commits

# Ordering guard for the drill: part 2 needs messages that life 1 never saw. If the produce has
# already happened, life 1 read them too and part 2 would have nothing to lose.
labelled = spark.sql(
    f"SELECT count_if({header_sql('ledgerline.run_id')} IS NOT NULL) AS n FROM {TRAP_TABLE}"
).collect()[0].n
assert labelled == 0, f"{labelled} labelled messages already on the topic: run part 1 BEFORE the produce"
print("OK — now run the two partial produces on the laptop, then drill1_trap_2_reset.")