-- Every unit Silver applied, newest first: a micro-batch for the two streams, a night for the dims.
-- One line per unit, from the three merge logs the lag alarm also reads.
SELECT 'orders' AS stream, concat('batch ', batch_id) AS unit, bronze_rows AS rows_in,
       inserted, updated, CAST(0 AS BIGINT) AS deleted, applied_at
FROM workspace.silver.orders_log
UNION ALL
SELECT 'inventory', concat('batch ', batch_id), bronze_rows, inserted, updated, deleted, applied_at
FROM workspace.silver.inventory_cdc_log
UNION ALL
SELECT dimension, concat('night ', dump_date), source_rows, inserted, updated, deleted, applied_at
FROM workspace.silver.dims_merge_log
ORDER BY applied_at DESC
