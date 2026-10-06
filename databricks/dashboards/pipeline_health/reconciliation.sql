-- The cross-source tie-out: per SKU, units ordered (one order_items row per unit) against units the
-- stock feed decremented. FULL OUTER JOIN: a SKU on one side only is a disagreement too.
-- Expected: 34,448 SKUs, 0 mismatched, 112,650 = 112,650 (merge_orders Verify 7, Drill 2).
WITH ordered AS (
  SELECT sku_key, count(*) AS units FROM workspace.silver.order_items GROUP BY sku_key
),
decremented AS (
  SELECT sku_key, sum(prev_stock_qty - stock_qty) AS units
  FROM workspace.silver.inventory_events WHERE prev_stock_qty > stock_qty GROUP BY sku_key
)
SELECT count(*)                                                AS skus,
       count_if(coalesce(o.units, 0) <> coalesce(d.units, 0))  AS skus_mismatched,
       sum(o.units)                                            AS units_ordered,
       sum(d.units)                                            AS units_decremented
FROM ordered o FULL OUTER JOIN decremented d USING (sku_key)
