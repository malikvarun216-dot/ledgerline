-- ledgerline — Snowflake day one: cost guards (Session 13)
--
-- Run as ACCOUNTADMIN, in a Snowsight worksheet, top to bottom. Safe to re-run: every object is
-- created only if missing, then set to the values below ("converge"), so a re-run repairs drift
-- instead of failing or resetting a monitor's spent credits (CREATE OR REPLACE would).
--
-- Why these guards exist (decisions.md, Session 13 "Day-one cost guards"):
--   * a warehouse bills per second while running, 60 s minimum per resume; idle = suspended = $0
--   * the trial made three warehouses by itself (COMPUTE_WH, SNOWFLAKE_LEARNING_WH,
--     SYSTEM$STREAMLIT_NOTEBOOK_WH) — nothing here started them, so the account-wide monitor and
--     the account-wide statement cap are what watch them
--   * a resource monitor counts WAREHOUSE credits only. Snowpipe, serverless tasks, clustering and
--     other serverless features are NOT counted — read those from ACCOUNT_USAGE (S21)

USE ROLE ACCOUNTADMIN;

-- 1. Account-wide ceiling: covers every warehouse in the account, including the side-effect ones.
--    ~15 credits expected for the whole Gold phase; 40 leaves room for S21's benchmarks.
CREATE RESOURCE MONITOR IF NOT EXISTS LEDGERLINE_ACCOUNT_RM
  WITH CREDIT_QUOTA = 40 FREQUENCY = MONTHLY START_TIMESTAMP = IMMEDIATELY;
ALTER RESOURCE MONITOR LEDGERLINE_ACCOUNT_RM SET
  CREDIT_QUOTA = 40
  TRIGGERS ON 50 PERCENT DO NOTIFY
           ON 80 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND             -- running queries finish, nothing new starts
           ON 110 PERCENT DO SUSPEND_IMMEDIATE;  -- running queries are cancelled
ALTER ACCOUNT SET RESOURCE_MONITOR = LEDGERLINE_ACCOUNT_RM;

-- 2. Daily spike guard for the project warehouse (the AWS `ledgerline-daily-spike` idea):
--    4 credits = 4 hours of an XSMALL running flat out in one day.
CREATE RESOURCE MONITOR IF NOT EXISTS LEDGERLINE_WH_DAILY_RM
  WITH CREDIT_QUOTA = 4 FREQUENCY = DAILY START_TIMESTAMP = IMMEDIATELY;
ALTER RESOURCE MONITOR LEDGERLINE_WH_DAILY_RM SET
  CREDIT_QUOTA = 4
  TRIGGERS ON 75 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND;

-- 3. One warehouse for all project work. Smallest size, stops after 60 s idle, wakes on demand,
--    never more than one cluster, and no single statement may run longer than 15 minutes
--    (the default is 172,800 s = 2 days).
CREATE WAREHOUSE IF NOT EXISTS LEDGERLINE_WH
  WAREHOUSE_SIZE = XSMALL INITIALLY_SUSPENDED = TRUE;
ALTER WAREHOUSE LEDGERLINE_WH SET
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 1
  STATEMENT_TIMEOUT_IN_SECONDS = 900
  STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 300
  RESOURCE_MONITOR = LEDGERLINE_WH_DAILY_RM
  COMMENT = 'ledgerline: all project work. 60 s auto-suspend, 15 min statement cap, daily monitor 4 credits.';

-- 4. The warehouses the trial created by itself: same 60 s idle stop. Kept, not dropped —
--    Snowsight features (worksheets, notebooks, the learning database) expect them.
ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 60;
ALTER WAREHOUSE SNOWFLAKE_LEARNING_WH SET AUTO_SUSPEND = 60;
ALTER WAREHOUSE "SYSTEM$STREAMLIT_NOTEBOOK_WH" SET AUTO_SUSPEND = 60;

-- 5. Account-wide statement cap: one hour, for every warehouse including the three above.
ALTER ACCOUNT SET STATEMENT_TIMEOUT_IN_SECONDS = 3600;

-- 6. Worksheets open on the project warehouse, not COMPUTE_WH.
ALTER USER ZUBA11 SET DEFAULT_WAREHOUSE = LEDGERLINE_WH;

-- 6b. Added later in Session 13, after the service user read every column of SHOW WAREHOUSES
--     (the worksheet had cut them off): every warehouse came with ENABLE_QUERY_ACCELERATION = TRUE
--     (scale factor 8) and generation 2. Query acceleration is SERVERLESS compute — billed
--     beside the warehouse, invisible to both resource monitors above — and it would silently
--     change the S21 measurements (caches, pruning, spilling). Off; S21 turns it on to measure it.
ALTER WAREHOUSE LEDGERLINE_WH SET ENABLE_QUERY_ACCELERATION = FALSE;
ALTER WAREHOUSE COMPUTE_WH SET ENABLE_QUERY_ACCELERATION = FALSE;
ALTER WAREHOUSE SNOWFLAKE_LEARNING_WH SET ENABLE_QUERY_ACCELERATION = FALSE;
ALTER WAREHOUSE "SYSTEM$STREAMLIT_NOTEBOOK_WH" SET ENABLE_QUERY_ACCELERATION = FALSE;

-- 6c. Serverless spend every user could start: the trial granted PUBLIC (the role every user
--     has) AI functions, the Cortex database role, and the CPU and GPU container compute pools.
--     None is counted by a resource monitor. The project uses none of them; a leaked service
--     key could. Revoked; a role that needs one later gets it by name.
REVOKE USE AI FUNCTIONS ON ACCOUNT FROM ROLE PUBLIC;
REVOKE DATABASE ROLE SNOWFLAKE.CORTEX_USER FROM ROLE PUBLIC;
REVOKE USAGE ON COMPUTE POOL SYSTEM_COMPUTE_POOL_CPU FROM ROLE PUBLIC;
REVOKE USAGE ON COMPUTE POOL SYSTEM_COMPUTE_POOL_GPU FROM ROLE PUBLIC;

-- 7. Check (read-only).
SHOW RESOURCE MONITORS;
SHOW WAREHOUSES;
SHOW PARAMETERS LIKE 'STATEMENT_TIMEOUT_IN_SECONDS' IN ACCOUNT;
