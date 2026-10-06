-- Per source: when Bronze last received data, and when Silver last applied some.
-- A Silver run that finds nothing new writes no log line, so `silver_last_applied` is "last time there
-- was something to apply", not "last time the Job ran" (the Job's own page shows that).
SELECT 'orders' AS source,
       (SELECT max(_ingested_at) FROM workspace.bronze.orders)        AS bronze_last_landed,
       (SELECT max(applied_at) FROM workspace.silver.orders_log)     AS silver_last_applied
UNION ALL
SELECT 'inventory_cdc',
       (SELECT max(_ingested_at) FROM workspace.bronze.inventory_cdc),
       (SELECT max(applied_at) FROM workspace.silver.inventory_cdc_log)
UNION ALL
SELECT 'customer',
       (SELECT max(_ingested_at) FROM workspace.bronze.customer),
       (SELECT max(applied_at) FROM workspace.silver.dims_merge_log WHERE dimension = 'customer')
UNION ALL
SELECT 'product',
       (SELECT max(_ingested_at) FROM workspace.bronze.product),
       (SELECT max(applied_at) FROM workspace.silver.dims_merge_log WHERE dimension = 'product')
UNION ALL
SELECT 'seller',
       (SELECT max(_ingested_at) FROM workspace.bronze.seller),
       (SELECT max(applied_at) FROM workspace.silver.dims_merge_log WHERE dimension = 'seller')
