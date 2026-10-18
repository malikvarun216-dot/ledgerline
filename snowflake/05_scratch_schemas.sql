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

-- Session 14: exp_05 builds its wrong-on-purpose snapshots here, as the transformer (dbt).
USE ROLE SYSADMIN;  
CREATE SCHEMA IF NOT EXISTS LEDGERLINE.S14
  COMMENT = 'Session 14 experiments: exp_05 (SCD2 run time vs business time), hard deletes. Scratch.';

USE ROLE SECURITYADMIN;
GRANT USAGE ON SCHEMA LEDGERLINE.S14 TO ROLE LEDGERLINE_TRANSFORMER;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA LEDGERLINE.S14 TO ROLE LEDGERLINE_TRANSFORMER;

SHOW GRANTS ON SCHEMA LEDGERLINE.S14;

-- Session 15: the MERGE experiment (snapshot by anti-join, change log by its flag; dbt's merge vs
-- merge_changes) builds its tables here, as the transformer. S13 and S14 are dropped: their
-- findings are in incidents.md / progress.md, and nothing reads them.
USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS LEDGERLINE.S15
  COMMENT = 'Session 15 experiments: MERGE + anti-join on a snapshot, in-band deletes in a dbt incremental. Scratch.';
DROP SCHEMA IF EXISTS LEDGERLINE.S13;
DROP SCHEMA IF EXISTS LEDGERLINE.S14;

USE ROLE SECURITYADMIN;
GRANT USAGE ON SCHEMA LEDGERLINE.S15 TO ROLE LEDGERLINE_TRANSFORMER;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA LEDGERLINE.S15 TO ROLE LEDGERLINE_TRANSFORMER;

SHOW GRANTS ON SCHEMA LEDGERLINE.S15;

-- Session 16: the clone experiment (zero-copy clone of a table; a clone of a past state; the
-- source dropped under its clone) builds here, as the transformer. S15 is dropped: its findings
-- are in incidents.md / progress.md (s15_merge, 46 of 46), and nothing reads it.
USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS LEDGERLINE.S16
  COMMENT = 'Session 16 experiments: zero-copy clone, clone at a past point, source dropped under a clone. Scratch.';
DROP SCHEMA IF EXISTS LEDGERLINE.S15;

USE ROLE SECURITYADMIN;
GRANT USAGE ON SCHEMA LEDGERLINE.S16 TO ROLE LEDGERLINE_TRANSFORMER;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA LEDGERLINE.S16 TO ROLE LEDGERLINE_TRANSFORMER;

SHOW GRANTS ON SCHEMA LEDGERLINE.S16;


-- Session 17: exp_06 (a change file loaded late, below the facts' high-water mark; a version
-- lookback; an arrival-time mark; dbt microbatch with lookback) builds here, as the transformer.
-- S16 is dropped: it holds only ORDERS_CLONE / ORDERS_BEFORE (99,441 rows each, 2026-10-12), the
-- clone experiment's tables; its findings are in progress.md / incidents.md (S16), nothing reads it.
USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS LEDGERLINE.S17
  COMMENT = 'Session 17 experiments: late change file vs high-water mark, lookback, arrival mark, microbatch. Scratch.';
DROP SCHEMA IF EXISTS LEDGERLINE.S16;

USE ROLE SECURITYADMIN;
GRANT USAGE ON SCHEMA LEDGERLINE.S17 TO ROLE LEDGERLINE_TRANSFORMER;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA LEDGERLINE.S17 TO ROLE LEDGERLINE_TRANSFORMER;

SHOW GRANTS ON SCHEMA LEDGERLINE.S17;
