-- Session 19b: the same answer key on Databricks Silver, the tables Genie answers from.
-- Run in the Databricks SQL Editor (whole file). Each number must equal Snowflake's
-- (snowflake/experiments/s19b_answer_key.sql: Q1 96,096 · Q2 50,864 / 49,554 · Q3 SP 40,143 ·
-- Q4 7,386,050.80 / 7,217,172.24 · Q5 1.1417 · Q6 cama_mesa_banho 10,955). A difference means
-- Silver and Gold disagree, and must be explained before any AI answer is graded.
--
-- On Silver the person is one join further away: orders carry customer_id only, and
-- silver.customer_order_link maps it to customer_unique_id (Gold made that join in dbt).
--
-- Genie's instructions (paste into the Genie space's "Instructions", the same facts the Snowflake
-- semantic view states):
--   A customer is a person: customer_unique_id. orders.customer_id is a per-order id, never a person;
--   join orders.customer_id to customer_order_link.customer_id to get customer_unique_id.
--   One row of order_items is one unit sold. Revenue is the sum of order_items.price; freight_value is
--   shipping, not revenue. "Sold in <year>" means orders whose created_at falls in that year.
--   product_category_name is in Portuguese.

SELECT 'Q1' AS q, COUNT(DISTINCT l.customer_unique_id) AS people_who_ordered,
       COUNT(DISTINCT o.customer_id) AS customer_ids_trap
FROM workspace.silver.orders o
LEFT JOIN workspace.silver.customer_order_link l USING (customer_id)
UNION ALL
SELECT 'Q2', COUNT(*), COUNT_IF(o.status = 'delivered')
FROM workspace.silver.order_items i JOIN workspace.silver.orders o USING (order_id)
WHERE YEAR(o.created_at) = 2017
UNION ALL
SELECT 'Q3 ' || customer_state, COUNT(*), NULL
FROM workspace.silver.customer GROUP BY customer_state HAVING COUNT(*) > 12000
UNION ALL
SELECT 'Q4', ROUND(SUM(i.price), 2), ROUND(SUM(IF(o.status = 'delivered', i.price, 0)), 2)
FROM workspace.silver.order_items i JOIN workspace.silver.orders o USING (order_id)
WHERE YEAR(o.created_at) = 2018
UNION ALL
SELECT 'Q5', ROUND((SELECT COUNT(*) FROM workspace.silver.order_items)
                   / (SELECT COUNT(DISTINCT order_id) FROM workspace.silver.order_items), 4),
             ROUND((SELECT COUNT(*) FROM workspace.silver.order_items)
                   / (SELECT COUNT(*) FROM workspace.silver.orders), 4)
UNION ALL
SELECT * FROM (
  SELECT 'Q6 ' || p.product_category_name, COUNT(*), COUNT(DISTINCT i.order_id)
  FROM workspace.silver.order_items i JOIN workspace.silver.product p USING (product_id)
  GROUP BY p.product_category_name ORDER BY 2 DESC LIMIT 1
);
