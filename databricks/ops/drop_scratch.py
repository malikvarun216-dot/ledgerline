# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Ops — remove experiment scratch tables from the production schemas
# MAGIC
# MAGIC Unity Catalog allows **100 tables per schema**. On 2026-10-06 `workspace.silver` held 84: 11
# MAGIC production tables and 73 scratch tables left by experiments and drills (exp_04 alone makes 36), and
# MAGIC Databricks emailed at 80% (incidents.md, 2026-10-06). At 100, the next `CREATE TABLE` there fails.
# MAGIC
# MAGIC **What it drops:** in `workspace.silver` and `workspace.bronze`, only tables whose names start with a
# MAGIC known scratch prefix. Production tables are listed **by name** and never touched. A table that is
# MAGIC neither stops the notebook before anything is dropped — nothing unknown is ever removed.
# MAGIC
# MAGIC **How to run:** "Run all" with the widget `mode` on `plan` (the default) prints the plan and drops
# MAGIC nothing. Set `mode` to `drop` and run again to drop. Every scratch table is rebuilt by its own
# MAGIC notebook on its next run, and a dropped managed table can be brought back for 7 days with
# MAGIC `UNDROP TABLE <name>`.

# COMMAND ----------

KEEP = {
    "silver": {
        "customer", "product", "seller", "dims_merge_log", "dims_rejected_rows",
        "inventory", "inventory_events", "inventory_cdc_log",
        "orders", "orders_log", "order_items",
    },
    "bronze": {
        "customer", "product", "seller", "orders", "orders_quarantine",
        "inventory_cdc", "inventory_cdc_quarantine", "stream_progress",
    },
}
SCRATCH_PREFIXES = {
    "silver": ("exp03_", "exp04_", "dx_", "drill2_", "s11_"),
    "bronze": ("exp01_", "exp02_", "drill1_"),
}
QUOTA = 100

dbutils.widgets.dropdown("mode", "plan", ["plan", "drop"])
MODE = dbutils.widgets.get("mode")


def tables(schema):
    return [r.table_name for r in spark.sql(
        f"SELECT table_name FROM workspace.information_schema.tables WHERE table_schema = '{schema}'"
    ).collect()]


plan = {}
for schema, keep in KEEP.items():
    names = tables(schema)
    scratch = sorted(n for n in names if n.startswith(SCRATCH_PREFIXES[schema]))
    unknown = sorted(set(names) - keep - set(scratch))
    clash = sorted(keep & set(scratch))
    missing = sorted(keep - set(names))
    print(f"workspace.{schema}: {len(names)} tables of {QUOTA} — keep {len(keep & set(names))}, "
          f"scratch {len(scratch)}, unknown {unknown}, production missing {missing}")
    assert not clash, f"a production name matches a scratch prefix: {clash}"
    assert not unknown, f"tables that are neither production nor known scratch — decide first: {unknown}"
    plan[schema] = scratch

for schema, scratch in plan.items():
    print(f"\nworkspace.{schema} — would drop {len(scratch)}:")
    print(", ".join(scratch))

# COMMAND ----------

if MODE != "drop":
    print("mode = plan: nothing dropped. Set the widget `mode` to `drop` and run again.")
else:
    for schema, scratch in plan.items():
        for name in scratch:
            spark.sql(f"DROP TABLE IF EXISTS workspace.{schema}.{name}")
    for schema in plan:
        left = tables(schema)
        print(f"workspace.{schema}: {len(left)} tables left — {sorted(left)}")
        assert set(left) == KEEP[schema] & set(left), "a scratch table survived"