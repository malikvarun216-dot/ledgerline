-- Session 19b: the answer key for the plain-English questions put to Cortex Analyst and Genie.
--
--   python scripts/sf.py --role LEDGERLINE_TRANSFORMER --file snowflake/experiments/s19b_answer_key.sql
--
-- Six questions, each with a grain trap: the obvious SQL gives a plausible, wrong number. Where a
-- question can be read two ways, every honest reading is computed, and an AI answer is graded right
-- only if its own SQL states the reading it used and the number matches that reading.
-- Read-only (GOLD), as the transformer (sees text in clear, like the semantic view's owner would).

-- Q1 "How many customers have placed at least one order?"
--    Trap: customer_id is one per ORDER in Olist; the person is customer_unique_id.
SELECT 'Q1' AS q,
       COUNT(DISTINCT customer_unique_id) AS people_who_ordered,   -- the answer
       COUNT(DISTINCT customer_id)        AS customer_ids_trap
FROM LEDGERLINE.GOLD.FCT_ORDERS;

-- Q2 "How many units were sold in 2017?"
--    Trap: counting orders, not items (one fct_order_items row = one unit).
--    Two honest readings: every order placed in 2017, or delivered orders only.
SELECT 'Q2' AS q,
       COUNT(*)                                             AS units_all_statuses,
       COUNT_IF(o.status = 'delivered')                     AS units_delivered,
       COUNT(DISTINCT o.order_id)                           AS orders_trap
FROM LEDGERLINE.GOLD.FCT_ORDER_ITEMS i
JOIN LEDGERLINE.GOLD.FCT_ORDERS o USING (order_id)
WHERE YEAR(o.created_at) = 2017;

-- Q3 "Which state has the most customers?"
--    Trap: counting customer_id rows in the orders fact instead of people.
SELECT 'Q3' AS q, customer_state, COUNT(*) AS people
FROM LEDGERLINE.GOLD.CUSTOMER_CURRENT
GROUP BY customer_state ORDER BY people DESC LIMIT 3;

-- Q4 "What was the total revenue in 2018, excluding freight?"
--    Trap: adding freight_value, or summing an order-level column that does not exist.
SELECT 'Q4' AS q,
       SUM(i.price)                                         AS revenue_all_statuses,
       SUM(IFF(o.status = 'delivered', i.price, 0))         AS revenue_delivered,
       SUM(i.price + i.freight_value)                       AS with_freight_trap
FROM LEDGERLINE.GOLD.FCT_ORDER_ITEMS i
JOIN LEDGERLINE.GOLD.FCT_ORDERS o USING (order_id)
WHERE YEAR(o.created_at) = 2018;

-- Q5 "What is the average number of items per order?"
--    Trap: the denominator. 775 orders have no item at all (canceled / unavailable).
SELECT 'Q5' AS q,
       ROUND((SELECT COUNT(*) FROM LEDGERLINE.GOLD.FCT_ORDER_ITEMS)
             / (SELECT COUNT(DISTINCT order_id) FROM LEDGERLINE.GOLD.FCT_ORDER_ITEMS), 4) AS per_order_with_items,
       ROUND((SELECT COUNT(*) FROM LEDGERLINE.GOLD.FCT_ORDER_ITEMS)
             / (SELECT COUNT(*) FROM LEDGERLINE.GOLD.FCT_ORDERS), 4)                      AS per_any_order;

-- Q6 "Which product category sold the most units?"
--    Trap: counting distinct products or orders instead of items. Category names are Portuguese.
SELECT 'Q6' AS q, p.product_category_name, COUNT(*) AS units, COUNT(DISTINCT i.order_id) AS orders_trap
FROM LEDGERLINE.GOLD.FCT_ORDER_ITEMS i
JOIN LEDGERLINE.GOLD.PRODUCT_CURRENT p USING (product_id)
GROUP BY 2 ORDER BY units DESC LIMIT 3;

-- Q7 (added during the run, to test "AI cannot write SQL that needs context and complex joins"):
-- "What share of São Paulo's 2018 revenue came from repeat customers (people with more than one
--  order ever)?" Three hops (items -> orders -> person -> state) plus a per-person count over ALL
-- years. Traps: counting repeat by customer_id (every id has exactly one order, so 0% repeat);
-- counting orders only in 2018; the state of the ORDER vs of the person (only the person has one).
WITH people AS (
  SELECT customer_unique_id, COUNT(*) AS orders_ever
  FROM LEDGERLINE.GOLD.FCT_ORDERS GROUP BY 1
), rev AS (
  SELECT c.customer_state, p.orders_ever, i.price
  FROM LEDGERLINE.GOLD.FCT_ORDER_ITEMS i
  JOIN LEDGERLINE.GOLD.FCT_ORDERS o USING (order_id)
  JOIN people p USING (customer_unique_id)
  JOIN LEDGERLINE.GOLD.CUSTOMER_CURRENT c USING (customer_unique_id)
  WHERE YEAR(o.created_at) = 2018 AND c.customer_state = 'SP'
)
SELECT 'Q7' AS q,
       ROUND(SUM(IFF(orders_ever > 1, price, 0)), 2)                   AS repeat_revenue,
       ROUND(SUM(price), 2)                                            AS sp_revenue,
       ROUND(100 * SUM(IFF(orders_ever > 1, price, 0)) / SUM(price), 2) AS repeat_share_pct
FROM rev;
-- Trap readings, for grading: repeat counted within 2018 only.
WITH p18 AS (
  SELECT customer_unique_id, COUNT(*) AS n FROM LEDGERLINE.GOLD.FCT_ORDERS
  WHERE YEAR(created_at) = 2018 GROUP BY 1
)
SELECT 'Q7 trap: repeat within 2018' AS q,
       ROUND(100 * SUM(IFF(p.n > 1, i.price, 0)) / SUM(i.price), 2) AS share_pct
FROM LEDGERLINE.GOLD.FCT_ORDER_ITEMS i
JOIN LEDGERLINE.GOLD.FCT_ORDERS o USING (order_id)
JOIN p18 p USING (customer_unique_id)
JOIN LEDGERLINE.GOLD.CUSTOMER_CURRENT c USING (customer_unique_id)
WHERE YEAR(o.created_at) = 2018 AND c.customer_state = 'SP';
