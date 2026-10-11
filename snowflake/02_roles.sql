-- ledgerline — Snowflake day one: roles, the database, the landing schema (Session 13)
--
-- Run in a Snowsight worksheet with the role picker on ACCOUNTADMIN, everything selected
-- (Ctrl+A, then Ctrl+Enter). Safe to re-run. Each block switches to the system role whose JOB
-- that step is — the separation is the lesson (decisions.md, Session 13 "Roles"):
--   USERADMIN     creates users and roles
--   SECURITYADMIN grants privileges (it holds MANAGE GRANTS)
--   SYSADMIN      owns databases, schemas and warehouses
--   ACCOUNTADMIN  account-level things only (monitors, integrations) — owns no data objects
--
-- Hierarchy (a role granted to another role gives it everything it can do):
--
--   SYSADMIN
--    ├── LEDGERLINE_LOADER        lands files into RAW (stages, COPY INTO, Snowpipe)
--    └── LEDGERLINE_TRANSFORMER   reads RAW, builds Gold (dbt, S14)
--         └── LEDGERLINE_ANALYST  reads Gold only (granted in S14, when Gold exists)

-- 0. The warehouse was created as ACCOUNTADMIN in 01_cost_guards.sql, so ACCOUNTADMIN owns it.
--    Hand it to SYSADMIN: the admin role should not own everyday objects.
USE ROLE ACCOUNTADMIN;
GRANT OWNERSHIP ON WAREHOUSE LEDGERLINE_WH TO ROLE SYSADMIN COPY CURRENT GRANTS;

-- Account default: UTC. A new account runs in America/Los_Angeles (the monitors' start_time read
-- "-0700" on 2026-10-11); Databricks Silver runs in UTC (_merge_dims.py). One clock for both
-- halves, so CURRENT_TIMESTAMP(), Snowsight and dbt snapshot times line up with Silver's.
ALTER ACCOUNT SET TIMEZONE = 'UTC';

-- 1. Roles.
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS LEDGERLINE_LOADER
  COMMENT = 'ledgerline: lands the Databricks bridge files into LEDGERLINE.RAW';
CREATE ROLE IF NOT EXISTS LEDGERLINE_TRANSFORMER
  COMMENT = 'ledgerline: reads RAW, builds Gold (dbt)';
CREATE ROLE IF NOT EXISTS LEDGERLINE_ANALYST
  COMMENT = 'ledgerline: reads Gold only';

-- 2. Hierarchy. Every custom role rolls up to SYSADMIN, so SYSADMIN can manage what they own.
GRANT ROLE LEDGERLINE_ANALYST     TO ROLE LEDGERLINE_TRANSFORMER;
GRANT ROLE LEDGERLINE_LOADER      TO ROLE SYSADMIN;
GRANT ROLE LEDGERLINE_TRANSFORMER TO ROLE SYSADMIN;
-- The human can switch into each one to test it from the inside.
GRANT ROLE LEDGERLINE_LOADER      TO USER ZUBA11;
GRANT ROLE LEDGERLINE_TRANSFORMER TO USER ZUBA11;
GRANT ROLE LEDGERLINE_ANALYST     TO USER ZUBA11;

-- 3. Database and the landing schema. RAW is a MANAGED ACCESS schema: the loader will own the
--    tables it creates, but only the schema owner (SYSADMIN) or a role with MANAGE GRANTS may
--    grant access to them — an owner cannot quietly share its own table.
USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS LEDGERLINE
  COMMENT = 'ledgerline Gold platform: RAW lands the bridge from Databricks Silver';
CREATE SCHEMA IF NOT EXISTS LEDGERLINE.RAW WITH MANAGED ACCESS
  COMMENT = 'Bridge files from Databricks Silver, loaded as they arrived';
ALTER SCHEMA LEDGERLINE.RAW ENABLE MANAGED ACCESS;

-- 4. Edition proof: a masking policy is an Enterprise feature. On Standard edition this CREATE
--    fails ("Unsupported feature"). A green line in Query History is the proof; it is dropped
--    straight away — real masking policies are Session 18.
CREATE OR REPLACE MASKING POLICY LEDGERLINE.RAW.S13_EDITION_PROBE AS (v STRING) RETURNS STRING -> v;
DROP MASKING POLICY LEDGERLINE.RAW.S13_EDITION_PROBE;

-- 5. Privileges.
USE ROLE SECURITYADMIN;
GRANT USAGE ON WAREHOUSE LEDGERLINE_WH TO ROLE LEDGERLINE_LOADER;
GRANT USAGE ON WAREHOUSE LEDGERLINE_WH TO ROLE LEDGERLINE_ANALYST;   -- the transformer inherits it
GRANT USAGE ON DATABASE LEDGERLINE     TO ROLE LEDGERLINE_LOADER;
GRANT USAGE ON DATABASE LEDGERLINE     TO ROLE LEDGERLINE_ANALYST;   -- the transformer inherits it

GRANT USAGE ON SCHEMA LEDGERLINE.RAW TO ROLE LEDGERLINE_LOADER;
GRANT CREATE TABLE, CREATE STAGE, CREATE FILE FORMAT, CREATE PIPE
  ON SCHEMA LEDGERLINE.RAW TO ROLE LEDGERLINE_LOADER;

-- The transformer reads every RAW table: those that exist now (ALL) and those created later
-- (FUTURE). A future grant never covers objects that already exist — both lines are needed.
GRANT USAGE ON SCHEMA LEDGERLINE.RAW TO ROLE LEDGERLINE_TRANSFORMER;
GRANT SELECT ON ALL TABLES    IN SCHEMA LEDGERLINE.RAW TO ROLE LEDGERLINE_TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA LEDGERLINE.RAW TO ROLE LEDGERLINE_TRANSFORMER;

-- 6. Check (read-only). Click each line in Query History to see its result.
SHOW SCHEMAS IN DATABASE LEDGERLINE;              -- RAW: options = MANAGED ACCESS
SHOW GRANTS TO ROLE LEDGERLINE_LOADER;
SHOW GRANTS TO ROLE LEDGERLINE_TRANSFORMER;
SHOW FUTURE GRANTS IN SCHEMA LEDGERLINE.RAW;
