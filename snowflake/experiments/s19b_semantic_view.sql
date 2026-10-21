-- Session 19b: the semantic view Cortex Analyst answers from. Run after the S19B block of
-- 05_scratch_schemas.sql:
--
--   python scripts/sf.py --role LEDGERLINE_TRANSFORMER --file snowflake/experiments/s19b_semantic_view.sql
--
-- A semantic view is the "what our words mean" layer: which table is which grain, how they join, and
-- what a metric counts. It holds no data. Each grain trap of the answer key
-- (s19b_answer_key.sql) is written down here once, in the definitions and comments, the way a data
-- team would: a person is customer_unique_id, a unit is one order-items row, revenue has no freight.
-- Genie gets the same facts as its instructions text, so the two tools are compared on equal
-- knowledge.

CREATE OR REPLACE SEMANTIC VIEW LEDGERLINE.S19B.ORDERS_SV
  TABLES (
    orders AS LEDGERLINE.GOLD.FCT_ORDERS
      PRIMARY KEY (order_id)
      COMMENT = 'One row per order. customer_id is a per-order id in this data, NOT a person; the person is customer_unique_id.',
    items AS LEDGERLINE.GOLD.FCT_ORDER_ITEMS
      PRIMARY KEY (order_id, order_item_id)
      COMMENT = 'One row per order item = one unit sold. price excludes freight; freight_value is shipping.',
    customers AS LEDGERLINE.GOLD.CUSTOMER_CURRENT
      PRIMARY KEY (customer_unique_id)
      COMMENT = 'One row per person (customer_unique_id), as of now.',
    products AS LEDGERLINE.GOLD.PRODUCT_CURRENT
      PRIMARY KEY (product_id)
      COMMENT = 'One row per product. Category names are Portuguese, e.g. cama_mesa_banho (bed, bath, table).'
  )
  RELATIONSHIPS (
    items_to_orders AS items (order_id) REFERENCES orders,
    orders_to_customers AS orders (customer_unique_id) REFERENCES customers,
    items_to_products AS items (product_id) REFERENCES products
  )
  FACTS (
    items.item_price AS price COMMENT = 'Price of one unit, without freight.',
    items.item_freight AS freight_value COMMENT = 'Shipping charged for the item; not revenue.'
  )
  DIMENSIONS (
    orders.order_status AS status COMMENT = 'delivered, shipped, canceled, approved, unavailable, created.',
    orders.order_created_at AS created_at COMMENT = 'When the order was placed; use its year for "sold in <year>".',
    orders.order_year AS YEAR(created_at) COMMENT = 'Year the order was placed.',
    customers.customer_state AS customer_state COMMENT = 'Two-letter Brazilian state of the person, e.g. SP.',
    products.product_category AS product_category_name COMMENT = 'Product category (Portuguese).'
  )
  METRICS (
    orders.order_count AS COUNT(orders.order_id) COMMENT = 'Number of orders.',
    orders.customers_who_ordered AS COUNT(DISTINCT orders.customer_unique_id)
      COMMENT = 'Number of people who placed at least one order. Never count customer_id.',
    customers.customer_count AS COUNT(customers.customer_unique_id) COMMENT = 'Number of people (customers) as of now.',
    items.units_sold AS COUNT(items.order_item_id) COMMENT = 'Units sold = order-item rows.',
    items.revenue AS SUM(items.price) COMMENT = 'Revenue = sum of item prices, excluding freight.'
  )
  COMMENT = 'Session 19b scratch: ledgerline orders for Cortex Analyst, graded against s19b_answer_key.sql.';

SHOW SEMANTIC VIEWS IN SCHEMA LEDGERLINE.S19B;
