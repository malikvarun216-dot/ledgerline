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


-- Session 18: governance before production — masking policies (direct and by tag), a row access
-- policy, tag propagation, what a dbt rebuild does to all of them; Time Travel and UNDROP. Built
-- here as the transformer, which may create tags and policies in S18 only (production policies
-- belong to a governance role, decided in S18). The analyst may read S18, to be tested as itself.
-- S17 is dropped: it holds exp_06's 5 tables and the two *_BEFORE_ARRIVAL backups (99,441 and
-- 112,650 rows, 2026-10-18); the arrival-mark change was verified at 0 rows different, findings are
-- in progress.md / incidents.md (S17), nothing reads it.
USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS LEDGERLINE.S18
  COMMENT = 'Session 18 experiments: masking (direct, by tag), row access, tag propagation, Time Travel, UNDROP. Scratch.';
DROP SCHEMA IF EXISTS LEDGERLINE.S17;

USE ROLE SECURITYADMIN;
GRANT USAGE ON SCHEMA LEDGERLINE.S18 TO ROLE LEDGERLINE_TRANSFORMER;
GRANT CREATE TABLE, CREATE VIEW, CREATE TAG, CREATE MASKING POLICY, CREATE ROW ACCESS POLICY
  ON SCHEMA LEDGERLINE.S18 TO ROLE LEDGERLINE_TRANSFORMER;
GRANT USAGE ON SCHEMA LEDGERLINE.S18 TO ROLE LEDGERLINE_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA LEDGERLINE.S18 TO ROLE LEDGERLINE_ANALYST;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA LEDGERLINE.S18 TO ROLE LEDGERLINE_ANALYST;

SHOW GRANTS ON SCHEMA LEDGERLINE.S18;


-- Session 19: Streams & Tasks, a Dynamic Table vs a materialized view, Snowpark (a Python UDF and a
-- stored procedure), and the re-run of s18b_escape under the database-wide mask. Built here as the
-- transformer. NO reader grant on S19: the database-wide mask (13_database_tag.sql) broke the row
-- policy and was reverted (incidents.md 2026-10-21), so until a design is proven the leak's
-- prevention is the rule — a schema that may hold copies of Gold is never readable by a reader role.
-- S18 is dropped: its 8 copies of Gold were dropped 2026-10-19 (incident "Masked Gold data
-- escaped"); what is left is the experiment's tags / policies, STATE_ACCESS (2 rows) and the Cortex
-- probe PROBE_SV. Findings are in progress.md / constraints.md (S18, S18b); S19b builds its own
-- semantic view on GOLD. Nothing reads S18.
USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS LEDGERLINE.S19
  COMMENT = 'Session 19 experiments: streams, tasks, dynamic table vs materialized view, Snowpark, mask escape re-run. Scratch.';
DROP SCHEMA IF EXISTS LEDGERLINE.S18;

USE ROLE SECURITYADMIN;
GRANT USAGE ON SCHEMA LEDGERLINE.S19 TO ROLE LEDGERLINE_TRANSFORMER;
GRANT CREATE TABLE, CREATE VIEW, CREATE STREAM, CREATE TASK, CREATE DYNAMIC TABLE,
      CREATE MATERIALIZED VIEW, CREATE FUNCTION, CREATE PROCEDURE
  ON SCHEMA LEDGERLINE.S19 TO ROLE LEDGERLINE_TRANSFORMER;

-- A task runs only if its owner may run tasks: an account-level privilege, so ACCOUNTADMIN grants it.
-- EXECUTE MANAGED TASK is for serverless tasks (no warehouse); granted so the experiment can show
-- one, the way Snowpipe is serverless. Revoked at the end of S19 with every task suspended.
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT TO ROLE LEDGERLINE_TRANSFORMER;

SHOW GRANTS ON SCHEMA LEDGERLINE.S19;
SHOW GRANTS TO ROLE LEDGERLINE_TRANSFORMER;

-- End of Session 19: every task in S19 suspended (verified by SHOW TASKS, and the dynamic table
-- DT_CURRENT suspended); the account-level right to run tasks goes back. Re-granted by the next
-- session that builds tasks, with this same pair at its end.
USE ROLE ACCOUNTADMIN;
REVOKE EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT FROM ROLE LEDGERLINE_TRANSFORMER;
SHOW GRANTS TO ROLE LEDGERLINE_TRANSFORMER;   -- no row with privilege EXECUTE TASK
