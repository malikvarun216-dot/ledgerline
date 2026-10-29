-- Session 24: the semantic view Cortex Analyst answers from, beside the copilot. Run after
-- snowflake/16_copilot_reader.sql (which makes schema S24):
--
--   python scripts/sf.py --role LEDGERLINE_TRANSFORMER --file copilot/semantic_view.sql
--
-- EQUAL KNOWLEDGE (the S19b rule): every comment below says what copilot/facts.py says for the same
-- table — the grain first, then the trap. Change one, change the other. A semantic view holds no
-- data; it is read at question time against GOLD, under the asking role's own masks and row policy.
-- S19b's ORDERS_SV covered the 4 tables of Q1–Q7; this one adds the 5 tables Q8–Q20 need.

CREATE OR REPLACE SEMANTIC VIEW LEDGERLINE.S24.GOLD_SV
  TABLES (
    orders AS LEDGERLINE.GOLD.FCT_ORDERS
      PRIMARY KEY (order_id)
      COMMENT = 'One row per order. customer_id is a per-ORDER id in this data, NOT a person: the person is customer_unique_id. status: delivered, shipped, canceled, approved, unavailable, created. created_at = when the order was placed (use its year for "in <year>").',
    items AS LEDGERLINE.GOLD.FCT_ORDER_ITEMS
      PRIMARY KEY (order_id, order_item_id)
      COMMENT = 'One row per order item = ONE UNIT SOLD. price = one unit''s price WITHOUT freight; freight_value is shipping, not revenue. Revenue = SUM(price). Some canceled / unavailable orders have no item at all.',
    customers AS LEDGERLINE.GOLD.CUSTOMER_CURRENT
      PRIMARY KEY (customer_unique_id)
      COMMENT = 'One row per person (customer_unique_id), as of now. customer_state = two-letter Brazilian state, e.g. SP. customer_city is masked (''***'') for readers.',
    products AS LEDGERLINE.GOLD.PRODUCT_CURRENT
      PRIMARY KEY (product_id)
      COMMENT = 'One row per product. product_category_name is Portuguese, e.g. cama_mesa_banho.',
    reviews AS LEDGERLINE.GOLD.FCT_REVIEWS
      PRIMARY KEY (review_id, order_id)
      COMMENT = 'One row per customer review. Neither review_id nor order_id is unique alone; the key is (review_id, order_id). Count reviews with COUNT(*). review_score 1-5. review_comment_message is masked (''***'') for readers.',
    promise AS LEDGERLINE.GOLD.DELIVERY_PROMISE
      PRIMARY KEY (order_id)
      COMMENT = 'One row per DELIVERED order: promised_days, actual_days, days_late (negative = early), is_late, lateness_bucket, review_score, reviewed_before_delivery, customer_state, purchase_month.',
    rating AS LEDGERLINE.GOLD.LATE_VS_RATING
      COMMENT = 'Delivered orders summed by purchase_month x customer_state x lateness_bucket. Averages are SUM(score_sum) / SUM(reviewed_orders); NEVER average the stored avg_score column (an average of averages). low_score_orders = scores 1-2.',
    scorecard AS LEDGERLINE.GOLD.SELLER_SCORECARD
      PRIMARY KEY (seller_id, purchase_month)
      COMMENT = 'One row per seller PER PURCHASE MONTH. tier (at risk, watch, good, too few orders) is decided per month, so a seller can be in several tiers; count sellers with COUNT(DISTINCT seller_id). revenue = SUM(price) of the seller''s items that month.',
    complaints AS LEDGERLINE.GOLD.COMPLAINT_TRENDS
      COMMENT = 'Reviews summed by review_month x topic x sentiment (an AI label of the review text). topic "no text" and "unreadable answer" are not complaints. Count with SUM(reviews).'
  )
  RELATIONSHIPS (
    items_to_orders AS items (order_id) REFERENCES orders,
    orders_to_customers AS orders (customer_unique_id) REFERENCES customers,
    items_to_products AS items (product_id) REFERENCES products
  )
  FACTS (
    items.item_price AS price COMMENT = 'Price of one unit, without freight.',
    items.item_freight AS freight_value COMMENT = 'Shipping charged for the item; not revenue.',
    promise.promised_days AS promised_days COMMENT = 'Days from purchase to the promised date.',
    promise.actual_days AS actual_days COMMENT = 'Days from purchase to delivery.',
    promise.days_late AS days_late COMMENT = 'Delivered minus promised, in days; negative = early.',
    promise.order_review_score AS review_score COMMENT = 'The order''s review score, 1-5 (null if not reviewed).',
    rating.delivered_orders AS delivered_orders COMMENT = 'Delivered orders in the cell.',
    rating.reviewed_orders AS reviewed_orders COMMENT = 'Reviewed delivered orders in the cell.',
    rating.score_sum AS score_sum COMMENT = 'Sum of review scores in the cell.',
    rating.low_score_orders AS low_score_orders COMMENT = 'Orders scored 1-2.',
    rating.stored_avg_score AS avg_score COMMENT = 'Per-cell average. Do NOT average it across cells.',
    scorecard.seller_revenue AS revenue COMMENT = 'The seller''s item revenue that month, no freight.',
    scorecard.seller_orders AS orders COMMENT = 'The seller''s orders that month.',
    complaints.review_rows AS reviews COMMENT = 'Reviews in the cell.'
  )
  DIMENSIONS (
    orders.order_status AS status COMMENT = 'delivered, shipped, canceled, approved, unavailable, created.',
    orders.order_created_at AS created_at COMMENT = 'When the order was placed; use its year for "in <year>".',
    orders.order_year AS YEAR(created_at) COMMENT = 'Year the order was placed.',
    orders.order_customer_unique_id AS customer_unique_id COMMENT = 'The person who placed the order.',
    customers.customer_state AS customer_state COMMENT = 'Two-letter Brazilian state of the person, e.g. SP.',
    customers.customer_city AS customer_city COMMENT = 'City of the person. Masked (''***'') for readers.',
    products.product_category AS product_category_name COMMENT = 'Product category (Portuguese).',
    reviews.review_id AS review_id COMMENT = 'Not unique alone.',
    reviews.review_score AS review_score COMMENT = 'Score 1-5.',
    reviews.review_comment_message AS review_comment_message COMMENT = 'Review text. Masked (''***'') for readers.',
    reviews.has_message AS has_message COMMENT = 'True if the review has text.',
    promise.promise_state AS customer_state COMMENT = 'Customer state of the delivered order.',
    promise.promise_month AS purchase_month COMMENT = 'Month the order was placed.',
    promise.is_late AS is_late COMMENT = 'Delivered after the promised date.',
    promise.lateness_bucket AS lateness_bucket COMMENT = 'early 8+ days ... late 8+ days.',
    promise.reviewed_before_delivery AS reviewed_before_delivery COMMENT = 'Reviewed before it arrived.',
    rating.rating_month AS purchase_month COMMENT = 'Purchase month of the cell.',
    rating.rating_state AS customer_state COMMENT = 'Customer state of the cell.',
    rating.rating_bucket AS lateness_bucket COMMENT = 'Lateness bucket of the cell.',
    rating.rating_is_late AS is_late COMMENT = 'Whether the cell is late deliveries.',
    scorecard.seller_id AS seller_id COMMENT = 'The seller.',
    scorecard.scorecard_month AS purchase_month COMMENT = 'Month of the scorecard row.',
    scorecard.tier AS tier COMMENT = 'at risk, watch, good, too few orders — per month.',
    complaints.review_month AS review_month COMMENT = 'Month the reviews were written.',
    complaints.topic AS topic COMMENT = 'AI topic: delivery, product, seller, price, other, no text, unreadable answer.',
    complaints.sentiment AS sentiment COMMENT = 'AI sentiment: positive, neutral, negative.'
  )
  METRICS (
    orders.order_count AS COUNT(orders.order_id) COMMENT = 'Number of orders.',
    orders.customers_who_ordered AS COUNT(DISTINCT orders.customer_unique_id)
      COMMENT = 'Number of people who placed at least one order. Never count customer_id.',
    customers.customer_count AS COUNT(customers.customer_unique_id) COMMENT = 'Number of people (customers) as of now.',
    items.units_sold AS COUNT(items.order_item_id) COMMENT = 'Units sold = order-item rows.',
    items.revenue AS SUM(items.price) COMMENT = 'Revenue = sum of item prices, excluding freight.',
    reviews.review_count AS COUNT(reviews.review_id) COMMENT = 'Number of reviews (rows), not distinct review_id.',
    promise.delivered_order_count AS COUNT(promise.order_id) COMMENT = 'Delivered orders.',
    promise.late_share AS COUNT_IF(promise.is_late) / COUNT(promise.order_id) COMMENT = 'Share of delivered orders that were late.',
    rating.avg_rating AS SUM(rating.score_sum) / SUM(rating.reviewed_orders)
      COMMENT = 'Average review score = sum of scores / reviewed orders. Never AVG(avg_score).',
    scorecard.seller_count AS COUNT(DISTINCT scorecard.seller_id) COMMENT = 'Distinct sellers.',
    scorecard.total_seller_revenue AS SUM(scorecard.revenue) COMMENT = 'Item revenue, no freight.',
    complaints.review_total AS SUM(complaints.reviews) COMMENT = 'Number of reviews.'
  )
  COMMENT = 'Session 24 scratch: ledgerline Gold for Cortex Analyst, graded against copilot/questions.yml beside the copilot.';

-- Readers ask Cortex under their own role; they need SELECT on this one view (USAGE on S24 comes from
-- 16_copilot_reader.sql). Never a grant on future tables: S24 holds no table.
GRANT SELECT ON SEMANTIC VIEW LEDGERLINE.S24.GOLD_SV TO ROLE LEDGERLINE_ANALYST;
GRANT SELECT ON SEMANTIC VIEW LEDGERLINE.S24.GOLD_SV TO ROLE LEDGERLINE_ANALYST_SP;

SHOW SEMANTIC VIEWS IN SCHEMA LEDGERLINE.S24;
