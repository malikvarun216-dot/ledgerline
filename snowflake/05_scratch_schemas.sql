-- ledgerline — Snowflake: a scratch schema per session's experiments (Session 13)
--
-- The Databricks rule from S11 (incidents.md 2026-10-06), on Snowflake: experiments never create
-- objects in RAW or any production schema. Each session gets LEDGERLINE.S<n>, owned by SYSADMIN,
-- where the functional roles may create what an experiment needs. Dropped when the session's
-- findings are recorded.
--
-- Run in a Snowsight worksheet, role picker on ACCOUNTADMIN, everything selected.

USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS LEDGERLINE.S13
  COMMENT = 'Session 13 experiments: COPY load history, ON_ERROR, VALIDATION_MODE, pipe vs COPY, keys. Scratch.';

USE ROLE SECURITYADMIN;
GRANT USAGE ON SCHEMA LEDGERLINE.S13 TO ROLE LEDGERLINE_LOADER;
GRANT CREATE TABLE, CREATE STAGE, CREATE FILE FORMAT, CREATE PIPE
  ON SCHEMA LEDGERLINE.S13 TO ROLE LEDGERLINE_LOADER;

SHOW GRANTS ON SCHEMA LEDGERLINE.S13;
