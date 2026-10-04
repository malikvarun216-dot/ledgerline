"""Lakeflow Declarative Pipeline (DLT) — inventory CDC with AUTO CDC, instead of a hand-written MERGE.

Session 11. One source file of the pipeline `ledgerline-dlt` (target: catalog `workspace`, schema
`silver_dlt`). A plain Python file, not a notebook: the pipeline runs it, nobody "runs all" on it.

**Why it exists.** Production Silver inventory (`databricks/silver/_merge_inventory_cdc`, Sessions 9-10)
is ~570 lines: dedup, a `seq` guard, deletes in-band, the newest event from the log, a refusal on tied
`seq`, a chain check, a merge log. Lakeflow's AUTO CDC (formerly `APPLY CHANGES`) claims the core of
that in one declaration. Building both on the same Bronze rows shows, with numbers, what the
declaration does for you and what it does not (decisions.md, Session 11).

What it declares, from Bronze `inventory_cdc` (158,725 rows):
- `inventory_cdc_checked` — every Bronze row, checked by expectations: the 53 denylisted events
  (159 copies) dropped, an unknown `op` fails the update, a missing `stock_qty` counted.
- `inventory_scd1` — current stock per SKU. AUTO CDC, SCD type 1: the declared twin of
  production's `silver.inventory`.
- `inventory_scd2` — every stock level a SKU ever had, one row per version; `__START_AT` /
  `__END_AT` are the `seq` that opened and closed it. AUTO CDC, SCD type 2.

Pipeline configuration (pipeline settings → Configuration):
- `ledgerline.repo_root` — the Git folder's path, to read the denylist. Required: a pipeline that
  cannot find the denylist must stop, not run without it.
- `ledgerline.apply_denylist` — "true" by default. "false" only for the deliberate run that feeds
  AUTO CDC the contaminated events (two different events at one `(sku_key, seq)`).
"""

import json

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession()

BRONZE_CDC = "workspace.bronze.inventory_cdc"
DENYLIST_FILE = "ops/incidents/2026-09-26_inventory_cdc_denylist.json"

# The same columns production Silver keeps (`EVENT_COLUMNS`); `change_reason` stays behind on purpose
# (decisions.md, Session 1: nothing in the pipeline may read it).
EVENT_COLUMNS = [
    "event_id", "event_ts", "produced_at", "op", "seq",
    "sku_key", "product_id", "seller_id", "stock_qty", "prev_stock_qty",
]
COORDINATES = ["_kafka_partition", "_kafka_offset"]


def _denylist():
    if spark.conf.get("ledgerline.apply_denylist", "true") != "true":
        return []
    root = spark.conf.get("ledgerline.repo_root")  # no default: missing config fails the update
    with open(f"{root}/{DENYLIST_FILE}") as fh:
        return json.load(fh)["event_ids"]


DENIED = _denylist()
NOT_DENIED = "event_id NOT IN ({})".format(", ".join(f"'{e}'" for e in DENIED)) if DENIED else "true"


# Three expectations, one of each action. An expectation is a rule on each row, checked as the row
# flows through: `expect` only counts the rows that break it, `expect_or_drop` removes them (and
# counts them), `expect_or_fail` stops the whole update at the first one, nothing written.
@dp.table(
    name="inventory_cdc_checked",
    comment="Bronze inventory_cdc, every row checked: denylisted events dropped, unknown op fails.",
)
@dp.expect_or_drop("not_denylisted", NOT_DENIED)
@dp.expect_or_fail("op_known", "op IN ('I', 'U', 'D')")
@dp.expect("stock_present", "stock_qty IS NOT NULL")
def inventory_cdc_checked():
    return spark.readStream.table(BRONZE_CDC).select(*EVENT_COLUMNS, *COORDINATES)


# SCD type 1 — one row per SKU, overwritten by each newer event, removed by a `D`. AUTO CDC orders the
# events per key by `sequence_by` (our `seq`), so an older event arriving late is ignored, like the
# `s.seq > t.seq` guard in production's MERGE. No in-batch dedup is written here on purpose: the 120
# re-sent copies reach AUTO CDC as they are, to see what it does with them.
dp.create_streaming_table(
    name="inventory_scd1",
    comment="Current stock per SKU, by AUTO CDC (SCD1). Compare with workspace.silver.inventory.",
)
dp.create_auto_cdc_flow(
    target="inventory_scd1",
    source="inventory_cdc_checked",
    keys=["sku_key"],
    sequence_by=F.col("seq"),
    apply_as_deletes=F.expr("op = 'D'"),
    except_column_list=[
        "op", "event_id", "event_ts", "produced_at", "prev_stock_qty", *COORDINATES,
    ],
    stored_as_scd_type=1,
)

# SCD type 2 — a new row for every change; the old row is closed, not overwritten. `__START_AT` /
# `__END_AT` take the type of `sequence_by`: here a `seq` (long), not a timestamp. `event_id` and
# `event_ts` are kept so each version names the event that opened it.
dp.create_streaming_table(
    name="inventory_scd2",
    comment="Every stock level per SKU, by AUTO CDC (SCD2). __START_AT / __END_AT are seq values.",
)
dp.create_auto_cdc_flow(
    target="inventory_scd2",
    source="inventory_cdc_checked",
    keys=["sku_key"],
    sequence_by=F.col("seq"),
    apply_as_deletes=F.expr("op = 'D'"),
    except_column_list=["op", "seq", "produced_at", "prev_stock_qty", *COORDINATES],
    stored_as_scd_type=2,
)
