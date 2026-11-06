-- ledgerline — Databricks: the four schemas dbt's `databricks` target builds into (Session 28)
--
-- Run by the HUMAN (the workspace owner) in the SQL Editor, on the Serverless Starter Warehouse:
-- select all, Run all. Re-runnable (IF NOT EXISTS; a repeated GRANT is a no-op).
--
-- Why the owner: on Snowflake SYSADMIN makes the schemas and dbt never creates one
-- (decisions.md S14); the same split here. dbt signs in as the `ledgerline-ci` service principal
-- (OAuth client id + secret from .env, application id below) and gets only what it builds with.
--
--   raw        the bridge's Parquet files, loaded by Databricks COPY INTO (scripts/dbx_raw.py) —
--              the same files Snowpipe loads into LEDGERLINE.RAW.SILVER_CHANGES, so both Golds
--              start from identical input (decisions.md S28)
--   staging    dbt views typing raw
--   snapshots  SCD2 history (dbt snapshots, one night per run: scripts/snapshot_nights.py)
--   gold       the facts, dims and marts
--
-- In Unity Catalog, CREATE TABLE on a schema covers views too; MODIFY lets COPY INTO and MERGE write;
-- READ FILES on the bridge's external location lets COPY INTO read s3://…/silver/ (the location's
-- credential, ledgerline_bridge_write, can also write; this grant cannot).

CREATE SCHEMA IF NOT EXISTS workspace.raw       COMMENT 'Bridge Parquet files as delivered (COPY INTO), for dbt target databricks';
CREATE SCHEMA IF NOT EXISTS workspace.staging   COMMENT 'dbt staging views over workspace.raw (target databricks)';
CREATE SCHEMA IF NOT EXISTS workspace.snapshots COMMENT 'dbt SCD2 snapshots (target databricks)';
CREATE SCHEMA IF NOT EXISTS workspace.gold      COMMENT 'dbt Gold (target databricks): facts, dims, marts';

GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA workspace.raw       TO `b21c5575-7fbe-433a-8d88-0c1f515f6ded`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA workspace.staging   TO `b21c5575-7fbe-433a-8d88-0c1f515f6ded`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA workspace.snapshots TO `b21c5575-7fbe-433a-8d88-0c1f515f6ded`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA workspace.gold      TO `b21c5575-7fbe-433a-8d88-0c1f515f6ded`;

GRANT READ FILES ON EXTERNAL LOCATION ledgerline_bridge TO `b21c5575-7fbe-433a-8d88-0c1f515f6ded`;

-- Expected: four rows for each schema below (USE SCHEMA, CREATE TABLE, SELECT, MODIFY to the
-- service principal), and READ FILES on the location.
SHOW GRANTS `b21c5575-7fbe-433a-8d88-0c1f515f6ded` ON SCHEMA workspace.gold;
SHOW GRANTS `b21c5575-7fbe-433a-8d88-0c1f515f6ded` ON EXTERNAL LOCATION ledgerline_bridge;
