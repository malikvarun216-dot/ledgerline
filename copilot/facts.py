"""What our words mean: the written facts both AIs get (Session 24).

The copilot reads these in its prompt; Cortex Analyst reads the same facts as comments in
``copilot/semantic_view.sql``. **Equal knowledge** (the S19b rule): if a fact is added here, it is
added there too, or the comparison measures the documentation instead of the tool.

Each table's note says its grain first — one row per what — because every trap in the answer key
is a grain slip: a ``customer_id`` counted as a person, an order counted as a unit, an average of
stored averages.
"""

from __future__ import annotations

DATABASE, SCHEMA = "LEDGERLINE", "GOLD"

TABLES: dict[str, str] = {
    "FCT_ORDERS": (
        "One row per order. customer_id is a per-ORDER id in this data, NOT a person: the person is "
        "customer_unique_id. status: delivered, shipped, canceled, approved, unavailable, created. "
        "created_at = when the order was placed (use its year for 'in <year>')."
    ),
    "FCT_ORDER_ITEMS": (
        "One row per order item = ONE UNIT SOLD. price = one unit's price WITHOUT freight; "
        "freight_value is shipping, not revenue. Revenue = SUM(price). Some canceled / unavailable "
        "orders have no item at all."
    ),
    "CUSTOMER_CURRENT": (
        "One row per person (customer_unique_id), as of now. customer_state = two-letter Brazilian "
        "state, e.g. SP. customer_city and customer_zip_code_prefix are masked ('***') for readers."
    ),
    "PRODUCT_CURRENT": ("One row per product. product_category_name is Portuguese, e.g. cama_mesa_banho."),
    "FCT_REVIEWS": (
        "One row per customer review. Neither review_id nor order_id is unique alone; the key is "
        "(review_id, order_id). Count reviews with COUNT(*). review_score 1-5. "
        "review_comment_title / review_comment_message are masked ('***') for readers."
    ),
    "DELIVERY_PROMISE": (
        "One row per DELIVERED order: promised_days, actual_days, days_late (negative = early), "
        "is_late, lateness_bucket (+ lateness_bucket_order for sorting), review_score, "
        "reviewed_before_delivery, customer_state, purchase_month."
    ),
    "LATE_VS_RATING": (
        "Delivered orders summed by purchase_month x customer_state x lateness_bucket. Averages are "
        "SUM(score_sum) / SUM(reviewed_orders); NEVER average the stored avg_score column "
        "(an average of averages). low_score_orders = scores 1-2."
    ),
    "SELLER_SCORECARD": (
        "One row per seller PER PURCHASE MONTH. tier: 'at risk', 'watch', 'good', 'too few orders' "
        "is decided per month, so a seller can be in several tiers; count sellers with "
        "COUNT(DISTINCT seller_id). revenue = SUM(price) of the seller's items that month."
    ),
    "COMPLAINT_TRENDS": (
        "Reviews summed by review_month x topic x sentiment (an AI label of the review text). "
        "topic 'no text' and 'unreadable answer' are not complaints. Count with SUM(reviews)."
    ),
}


def prompt_block(columns: dict[str, list[tuple[str, str]]]) -> str:
    """The schema the model sees: each table's note, then the columns the READER can see.

    ``columns`` comes from INFORMATION_SCHEMA as the reader role, so a table the role cannot read is
    not in it and is not offered to the model.
    """
    lines = [f"Database {DATABASE}, schema {SCHEMA}. Always write {DATABASE}.{SCHEMA}.<TABLE>.", ""]
    for table, note in TABLES.items():
        cols = columns.get(table)
        if not cols:
            continue
        lines.append(f"{DATABASE}.{SCHEMA}.{table} — {note}")
        lines.append("  columns: " + ", ".join(f"{c} {t}" for c, t in cols))
    return "\n".join(lines)
