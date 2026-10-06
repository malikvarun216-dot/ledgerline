-- Rows Silver applied per day and stream (input rows: Bronze messages for the streams, file rows for
-- the dims nights).
SELECT date(applied_at) AS day, stream, sum(rows_in) AS rows_in
FROM (
  SELECT 'orders' AS stream, bronze_rows AS rows_in, applied_at FROM workspace.silver.orders_log
  UNION ALL
  SELECT 'inventory', bronze_rows, applied_at FROM workspace.silver.inventory_cdc_log
  UNION ALL
  SELECT dimension, source_rows, applied_at FROM workspace.silver.dims_merge_log
)
GROUP BY ALL
ORDER BY day, stream
