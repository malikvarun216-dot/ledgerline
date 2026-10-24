-- ledgerline — Snowflake: the identity and warehouse Power BI reads Gold with (Session 23)
--
-- Run in a Snowsight worksheet, role picker on ACCOUNTADMIN, Ctrl+A, run. Then run the ALTER USER
-- line that `python scripts/sf_keypair.py --user LEDGERLINE_BI_SVC --out ~/.snowflake/ledgerline_bi_svc.p8`
-- printed (it carries the public key). decisions.md, Session 23.
--
-- Why a user of its own: Power BI's "Role" box is free text, so the report gets whatever the USER
-- holds. LEDGERLINE_SVC holds TRANSFORMER (clear text, every state) — a typo or a curious author
-- would see past every mask. This user holds the two reader roles and nothing else; secondary roles
-- off, so a session has exactly one of them.
--
-- Why a warehouse of its own: a dashboard refresh fires many small queries at once; on the shared
-- XSMALL they would queue behind dbt (S21: queues are cured by clusters, not size), and its credits
-- could not be told apart from dbt's. Same guards as LEDGERLINE_WH, a smaller daily monitor.

-- 1. The warehouse (SYSADMIN owns objects) and its daily spike guard (monitors: ACCOUNTADMIN).
USE ROLE ACCOUNTADMIN;
CREATE RESOURCE MONITOR IF NOT EXISTS LEDGERLINE_BI_DAILY_RM
  WITH CREDIT_QUOTA = 1 FREQUENCY = DAILY START_TIMESTAMP = IMMEDIATELY;
ALTER RESOURCE MONITOR LEDGERLINE_BI_DAILY_RM SET
  CREDIT_QUOTA = 1
  TRIGGERS ON 75 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND;

USE ROLE SYSADMIN;
CREATE WAREHOUSE IF NOT EXISTS LEDGERLINE_BI_WH
  WAREHOUSE_SIZE = XSMALL INITIALLY_SUSPENDED = TRUE;
ALTER WAREHOUSE LEDGERLINE_BI_WH SET
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 1
  STATEMENT_TIMEOUT_IN_SECONDS = 300
  STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 120
  ENABLE_QUERY_ACCELERATION = FALSE
  COMMENT = 'ledgerline: Power BI reads of Gold only (S23). 60 s auto-suspend, 5 min statement cap, daily monitor 1 credit.';

USE ROLE ACCOUNTADMIN;
ALTER WAREHOUSE LEDGERLINE_BI_WH SET RESOURCE_MONITOR = LEDGERLINE_BI_DAILY_RM;

USE ROLE SECURITYADMIN;
GRANT USAGE ON WAREHOUSE LEDGERLINE_BI_WH TO ROLE LEDGERLINE_ANALYST;
GRANT USAGE ON WAREHOUSE LEDGERLINE_BI_WH TO ROLE LEDGERLINE_ANALYST_SP;

-- 2. The user: a program, not a person — key pair only, never a password, never MFA.
USE ROLE USERADMIN;
CREATE USER IF NOT EXISTS LEDGERLINE_BI_SVC
  TYPE = SERVICE
  COMMENT = 'ledgerline Power BI Desktop (S23): key pair only, reader roles only';
ALTER USER LEDGERLINE_BI_SVC SET
  DEFAULT_ROLE = LEDGERLINE_ANALYST
  DEFAULT_WAREHOUSE = LEDGERLINE_BI_WH
  DEFAULT_NAMESPACE = LEDGERLINE.GOLD
  DEFAULT_SECONDARY_ROLES = ();

USE ROLE SECURITYADMIN;
GRANT ROLE LEDGERLINE_ANALYST    TO USER LEDGERLINE_BI_SVC;
GRANT ROLE LEDGERLINE_ANALYST_SP TO USER LEDGERLINE_BI_SVC;

-- Check: TYPE = SERVICE, HAS_PASSWORD = false, DEFAULT_SECONDARY_ROLES = [], and (after the
-- ALTER USER ... RSA_PUBLIC_KEY line) RSA_PUBLIC_KEY_FP = the fingerprint the script printed.
-- Then: exactly two role grants, both readers.
DESC USER LEDGERLINE_BI_SVC;
SHOW GRANTS TO USER LEDGERLINE_BI_SVC;
