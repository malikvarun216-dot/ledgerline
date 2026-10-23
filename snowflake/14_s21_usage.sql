-- Session 21: what the account's own history says about S21 — cost per warehouse, per query and per
-- serverless service, and who read what. Run by the human in Snowsight as ACCOUNTADMIN (Ctrl+A first),
-- at least ~3 hours after s21_performance.py ran: ACCOUNT_USAGE views lag 45 min to 3 h
-- (QUERY_ATTRIBUTION_HISTORY up to 8 h). The window is pinned to S21's start (2026-10-21 UTC): the
-- first version said "last 2 days", and run two days later it missed the main run (20:15-20:45 UTC).
-- The pipeline roles never read these views
-- (decisions.md, "S21 measures performance …"): they show every user's queries.
--
-- INFORMATION_SCHEMA vs ACCOUNT_USAGE, in one line each:
--   INFORMATION_SCHEMA  live, per database, 7-14 days, only what your role can see (the script used it)
--   ACCOUNT_USAGE       lags, whole account, 365 days, includes dropped objects (this file)

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE LEDGERLINE_WH;

-- 1. Warehouse credits, last 2 days: compute vs cloud services, per warehouse.
--    Expect LEDGERLINE_S21_WH to carry the session's cost; its daily monitor is 6 credits.
SELECT warehouse_name,
       ROUND(SUM(credits_used_compute), 3)        AS compute_credits,
       ROUND(SUM(credits_used_cloud_services), 3) AS cloud_services_credits,
       ROUND(SUM(credits_used), 3)                AS total_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= '2026-10-21 00:00:00'::TIMESTAMP_LTZ
GROUP BY 1 ORDER BY total_credits DESC;

-- 2. Serverless spend, last 2 days, by service: what NO resource monitor sees
--    (automatic clustering from experiment A, Snowpipe, serverless tasks, AI services).
SELECT service_type, ROUND(SUM(credits_used), 3) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
WHERE start_time >= '2026-10-21 00:00:00'::TIMESTAMP_LTZ
  AND service_type <> 'WAREHOUSE_METERING'
GROUP BY 1 ORDER BY credits DESC;

-- 3. Automatic clustering on S21.LINEITEM_RANDOM: credits and bytes moved per run.
SELECT start_time, end_time, ROUND(credits_used, 3) AS credits,
       num_bytes_reclustered, num_rows_reclustered
FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
WHERE database_name = 'LEDGERLINE' AND schema_name = 'S21' AND table_name = 'LINEITEM_RANDOM'
ORDER BY start_time;

-- 4. The S21 queries as the account sees them: the Query Profile's headline numbers in one row each.
SELECT start_time, ROUND(total_elapsed_time / 1000, 1) AS secs, warehouse_size, cluster_number,
       partitions_scanned, partitions_total, percentage_scanned_from_cache AS pct_cache,
       bytes_spilled_to_local_storage AS spill_local, bytes_spilled_to_remote_storage AS spill_remote,
       ROUND(queued_overload_time / 1000, 1) AS queued_s, query_tag, LEFT(query_text, 80) AS query
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE warehouse_name = 'LEDGERLINE_S21_WH'
  AND start_time >= '2026-10-21 00:00:00'::TIMESTAMP_LTZ
ORDER BY total_elapsed_time DESC
LIMIT 25;

-- 5. Credits per query (compute only, idle time not charged to any query): the 10 dearest S21 queries.
SELECT q.start_time, ROUND(a.credits_attributed_compute, 4) AS credits, q.query_tag,
       LEFT(q.query_text, 80) AS query
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY a
JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q ON q.query_id = a.query_id
WHERE a.warehouse_name = 'LEDGERLINE_S21_WH'
  AND a.start_time >= '2026-10-21 00:00:00'::TIMESTAMP_LTZ
ORDER BY credits DESC
LIMIT 10;

-- 6. ACCESS_HISTORY, who read what: every role and user that read a GOLD column in the last 7 days.
--    base_objects_accessed = the tables actually read underneath, even through a view.
SELECT ah.user_name, q.role_name, obj.value:"objectName"::STRING AS object_name,
       col.value:"columnName"::STRING AS column_name, COUNT(*) AS reads
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah
JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q ON q.query_id = ah.query_id,
     LATERAL FLATTEN(ah.base_objects_accessed) obj,
     LATERAL FLATTEN(obj.value:"columns") col
WHERE ah.query_start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND obj.value:"objectName"::STRING LIKE 'LEDGERLINE.GOLD.%'
GROUP BY 1, 2, 3, 4
ORDER BY reads DESC
LIMIT 30;

-- 7. ACCESS_HISTORY as lineage: what each S21 write was made from (objects_modified + its sources).
--    OUTER => TRUE on the sources (added 2026-10-24): the first run returned 0 rows. A plain FLATTEN of
--    an EMPTY array drops the row, and the S21 copies were read from SNOWFLAKE_SAMPLE_DATA, a share;
--    if reads of a share are not recorded, read_from is empty and every row vanished. Now it shows [].
SELECT ah.query_start_time, mod.value:"objectName"::STRING AS written,
       ARRAY_AGG(DISTINCT src.value:"objectName"::STRING) AS read_from,
       ARRAY_SIZE(ah.base_objects_accessed) AS sources_recorded
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
     LATERAL FLATTEN(ah.objects_modified) mod,
     LATERAL FLATTEN(ah.base_objects_accessed, OUTER => TRUE) src
WHERE ah.query_start_time >= '2026-10-21 00:00:00'::TIMESTAMP_LTZ
  AND mod.value:"objectName"::STRING LIKE 'LEDGERLINE.S21.%'
GROUP BY 1, 2, 4
ORDER BY 1;
