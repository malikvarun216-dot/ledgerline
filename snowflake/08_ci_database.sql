-- ledgerline — Snowflake: where dbt CI builds, and the robot that builds there (Session 16)
--
-- Run in a Snowsight worksheet, role picker on ACCOUNTADMIN, everything selected (Ctrl+A, then
-- Ctrl+Enter). Safe to re-run. Then run the ALTER USER line that
--   python scripts/sf_keypair.py --user LEDGERLINE_CI_SVC --out ~/.snowflake/ledgerline_ci_svc.p8
-- printed (it carries the PUBLIC key).
--
-- decisions.md, Session 16 "dbt CI builds land in a database of their own": slim CI builds the
-- models a pull request changed, one fresh schema per run, and reads everything else from
-- production (--defer). Those schemas live in LEDGERLINE_CI, never in LEDGERLINE — production keeps
-- "dbt creates no schemas" (Session 14). The CI role may create schemas here and nowhere else, and
-- may only READ production.
--
--   LEDGERLINE_CI            database, SYSADMIN-owned; no time travel (throwaway builds)
--   LEDGERLINE_CI_RUNNER     role: CREATE SCHEMA on LEDGERLINE_CI; SELECT on production's
--                            RAW / STAGING / SNAPSHOTS / GOLD (what --defer and clones read)
--   LEDGERLINE_CI_SVC        service user, key pair, that role only, secondary roles off

USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS LEDGERLINE_CI
  DATA_RETENTION_TIME_IN_DAYS = 0
  COMMENT = 'dbt CI builds (slim CI, one schema per run, dropped after). Never read by anyone.';

USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS LEDGERLINE_CI_RUNNER
  COMMENT = 'dbt CI: builds in LEDGERLINE_CI, reads production, writes nothing in LEDGERLINE';
CREATE USER IF NOT EXISTS LEDGERLINE_CI_SVC
  TYPE = SERVICE
  COMMENT = 'ledgerline GitHub Actions slim CI: key pair only, LEDGERLINE_CI_RUNNER only';
ALTER USER LEDGERLINE_CI_SVC SET
  DEFAULT_ROLE = LEDGERLINE_CI_RUNNER
  DEFAULT_WAREHOUSE = LEDGERLINE_WH
  DEFAULT_SECONDARY_ROLES = ();          -- one role per session (incidents.md 2026-10-11)
GRANT ROLE LEDGERLINE_CI_RUNNER TO USER LEDGERLINE_CI_SVC;
GRANT ROLE LEDGERLINE_CI_RUNNER TO ROLE SYSADMIN;   -- custom roles roll up to SYSADMIN (Session 13)

USE ROLE SECURITYADMIN;
GRANT USAGE ON WAREHOUSE LEDGERLINE_WH TO ROLE LEDGERLINE_CI_RUNNER;

-- Where it may build: its own schemas, in this database only. It owns what it creates.
GRANT USAGE, CREATE SCHEMA ON DATABASE LEDGERLINE_CI TO ROLE LEDGERLINE_CI_RUNNER;

-- What it may read: production, for --defer (an unchanged model is read where production built it)
-- and for zero-copy clones (cloning a table needs SELECT on it). Nothing else: no CREATE, no MODIFY.
GRANT USAGE ON DATABASE LEDGERLINE TO ROLE LEDGERLINE_CI_RUNNER;
GRANT USAGE ON SCHEMA LEDGERLINE.RAW       TO ROLE LEDGERLINE_CI_RUNNER;
GRANT USAGE ON SCHEMA LEDGERLINE.STAGING   TO ROLE LEDGERLINE_CI_RUNNER;
GRANT USAGE ON SCHEMA LEDGERLINE.SNAPSHOTS TO ROLE LEDGERLINE_CI_RUNNER;
GRANT USAGE ON SCHEMA LEDGERLINE.GOLD      TO ROLE LEDGERLINE_CI_RUNNER;
GRANT SELECT ON ALL TABLES    IN SCHEMA LEDGERLINE.RAW       TO ROLE LEDGERLINE_CI_RUNNER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA LEDGERLINE.RAW       TO ROLE LEDGERLINE_CI_RUNNER;
GRANT SELECT ON ALL VIEWS     IN SCHEMA LEDGERLINE.STAGING   TO ROLE LEDGERLINE_CI_RUNNER;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA LEDGERLINE.STAGING   TO ROLE LEDGERLINE_CI_RUNNER;
GRANT SELECT ON ALL TABLES    IN SCHEMA LEDGERLINE.SNAPSHOTS TO ROLE LEDGERLINE_CI_RUNNER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA LEDGERLINE.SNAPSHOTS TO ROLE LEDGERLINE_CI_RUNNER;
GRANT SELECT ON ALL TABLES    IN SCHEMA LEDGERLINE.GOLD      TO ROLE LEDGERLINE_CI_RUNNER;
GRANT SELECT ON ALL VIEWS     IN SCHEMA LEDGERLINE.GOLD      TO ROLE LEDGERLINE_CI_RUNNER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA LEDGERLINE.GOLD      TO ROLE LEDGERLINE_CI_RUNNER;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA LEDGERLINE.GOLD      TO ROLE LEDGERLINE_CI_RUNNER;

-- Check (read-only). Click each line in Query History to see its result.
SHOW GRANTS TO ROLE LEDGERLINE_CI_RUNNER;   -- USAGE/CREATE SCHEMA on LEDGERLINE_CI; USAGE + SELECT on LEDGERLINE
DESC USER LEDGERLINE_CI_SVC;                -- TYPE SERVICE, DEFAULT_SECONDARY_ROLES [], RSA_PUBLIC_KEY_FP after the ALTER USER
