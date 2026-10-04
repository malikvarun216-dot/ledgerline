"""Lakeflow Declarative Pipeline (DLT) — expectations on the order events.

Session 11. The second source file of the pipeline `ledgerline-dlt` (schema `workspace.silver_dlt`).

**Why it exists.** Session 7 chose `BACKWARD_TRANSITIVE` for the order schema. That contract lets a
producer *remove* a field, and Bronze would then land a blank `order_status` with no error. The
guard was bound to Session 11 as an expectation: `order_status` is never blank. Since Session 10
Silver derives `status` from the step columns and never reads `order_status`, so a blank one would
not change a Silver value. The expectation stays as the place a removed field is *counted*.

The second rule is a real defect in the data: 775 orders were placed with no line items (Olist's own
rows; Silver keeps them with `units = 0`). `ledgerline.items_rule` picks what the rule does with
them, so the three expectation actions run on the same real rows:
- `warn` (default) — rows kept, the 775 counted
- `drop` — rows removed, counted
- `fail` — the update stops at the first one, nothing written

Changing the rule applies only to rows not yet processed: a streaming table does not re-read old
rows. Seeing a new rule on old rows needs a full refresh of `orders_checked` (it is rebuilt from
Bronze; nothing in production reads it).
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

BRONZE_ORDERS = "workspace.bronze.orders"
EVENT_TYPES = ("created", "approved", "shipped", "delivered", "canceled", "unavailable")
COLUMNS = [
    "event_id", "event_ts", "produced_at", "event_type", "order_id", "customer_id", "order_status",
    "ts_is_synthetic", "estimated_delivery_date", "items", "_kafka_partition", "_kafka_offset",
]

ACTIONS = {"warn": dp.expect, "drop": dp.expect_or_drop, "fail": dp.expect_or_fail}
ITEMS_RULE = spark.conf.get("ledgerline.items_rule", "warn")
if ITEMS_RULE not in ACTIONS:
    raise ValueError(f"ledgerline.items_rule must be one of {sorted(ACTIONS)}, not {ITEMS_RULE!r}")


@dp.table(
    name="orders_checked",
    comment="Bronze orders, every event checked: status present, type known, created with items.",
)
@dp.expect("order_status_present", "order_status IS NOT NULL AND trim(order_status) <> ''")
@dp.expect_or_fail(
    "event_type_known", "event_type IN ({})".format(", ".join(f"'{t}'" for t in EVENT_TYPES))
)
@ACTIONS[ITEMS_RULE]("created_has_items", "event_type <> 'created' OR items IS NOT NULL")
def orders_checked():
    return spark.readStream.table(BRONZE_ORDERS).select(*COLUMNS)
