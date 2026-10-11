-- ledgerline — Snowflake: what has been spent, read from Snowflake's own usage views (Session 14)
--
-- Run in a Snowsight worksheet with the role picker on ACCOUNTADMIN, everything selected
-- (Ctrl+A, then Ctrl+Enter). Read-only. Click each line in Query History to see its result.
--
-- Why SQL and not the Cost management page: the page's place in the menu moves between Snowsight
-- versions; these views do not, and their numbers can be pasted into progress.md as they are.
-- ACCOUNT_USAGE views lag by up to ~3 hours, and only ACCOUNTADMIN (or a role granted the SNOWFLAKE
-- database) can read them — which is why LEDGERLINE_SVC got 0 rows in Session 13.

USE ROLE ACCOUNTADMIN;

-- 1. Credits per day and per kind of compute (warehouse, Snowpipe, cloud services, ...).
--    CREDITS_BILLED is what the trial balance is charged: cloud services are free up to 10% of
--    the day's warehouse credits, so CREDITS_USED can be higher than CREDITS_BILLED.
SELECT USAGE_DATE, SERVICE_TYPE,
       ROUND(SUM(CREDITS_USED), 4)   AS credits_used,
       ROUND(SUM(CREDITS_BILLED), 4) AS credits_billed
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
GROUP BY 1, 2
ORDER BY 1, 2;

-- 2. Per warehouse: which one spent it (the trial's own warehouses should be 0).
SELECT WAREHOUSE_NAME,
       ROUND(SUM(CREDITS_USED), 4) AS credits_used,
       MIN(START_TIME) AS first_hour, MAX(END_TIME) AS last_hour
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
GROUP BY 1
ORDER BY 2 DESC;

-- 3. Snowpipe: serverless, so neither resource monitor sees it (decisions.md, Session 13).
SELECT PIPE_NAME,
       ROUND(SUM(CREDITS_USED), 4) AS credits_used,
       SUM(FILES_INSERTED) AS files, SUM(BYTES_INSERTED) AS bytes
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
GROUP BY 1;

-- 4. The resource monitors' own count (credits this month / today, against their quotas).
SHOW RESOURCE MONITORS;
