-- ledgerline — Snowflake: the schemas dbt builds Gold in, and who may read them (Session 14)
--
-- Run in a Snowsight worksheet with the role picker on ACCOUNTADMIN, everything selected
-- (Ctrl+A, then Ctrl+Enter). Safe to re-run. Same separation as 02_roles.sql: SYSADMIN owns the
-- schemas, SECURITYADMIN grants (decisions.md, Session 14 "Gold schemas are made by SYSADMIN").
--
--   LEDGERLINE.STAGING    views that type RAW.SILVER_CHANGES, one per Silver table (dbt)
--   LEDGERLINE.SNAPSHOTS  dbt snapshot tables — the SCD2 history of the three dimensions
--   LEDGERLINE.GOLD       what an analyst reads: current state, dimensions, facts
--
-- All three are MANAGED ACCESS, like RAW: dbt (the transformer) owns the tables it creates, but
-- cannot grant them to anyone. Who reads GOLD is decided here, once, by future grants — not by
-- each model's `grants:` config (which would fail anyway: the owner of a table in a managed-access
-- schema has no right to grant it).

USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS LEDGERLINE.STAGING WITH MANAGED ACCESS
  COMMENT = 'dbt staging: RAW.SILVER_CHANGES typed, one view per Silver table';
CREATE SCHEMA IF NOT EXISTS LEDGERLINE.SNAPSHOTS WITH MANAGED ACCESS
  COMMENT = 'dbt snapshots: SCD2 history of customer, product, seller';
CREATE SCHEMA IF NOT EXISTS LEDGERLINE.GOLD WITH MANAGED ACCESS
  COMMENT = 'Gold: what analysts read';

USE ROLE SECURITYADMIN;
-- The transformer builds in all three. CREATE VIEW for staging and for dbt's temporary views
-- (an incremental MERGE stages its new rows in a `<model>__dbt_tmp` view); CREATE TABLE for tables,
-- snapshots and the temporary tables a snapshot run makes.
GRANT USAGE ON SCHEMA LEDGERLINE.STAGING   TO ROLE LEDGERLINE_TRANSFORMER;
GRANT USAGE ON SCHEMA LEDGERLINE.SNAPSHOTS TO ROLE LEDGERLINE_TRANSFORMER;
GRANT USAGE ON SCHEMA LEDGERLINE.GOLD      TO ROLE LEDGERLINE_TRANSFORMER;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA LEDGERLINE.STAGING   TO ROLE LEDGERLINE_TRANSFORMER;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA LEDGERLINE.SNAPSHOTS TO ROLE LEDGERLINE_TRANSFORMER;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA LEDGERLINE.GOLD      TO ROLE LEDGERLINE_TRANSFORMER;

-- The analyst reads GOLD only — not staging, not the snapshot tables underneath. Both lines are
-- needed: ALL covers what exists now (nothing yet), FUTURE covers what dbt creates later.
GRANT USAGE ON SCHEMA LEDGERLINE.GOLD TO ROLE LEDGERLINE_ANALYST;
GRANT SELECT ON ALL TABLES    IN SCHEMA LEDGERLINE.GOLD TO ROLE LEDGERLINE_ANALYST;
GRANT SELECT ON ALL VIEWS     IN SCHEMA LEDGERLINE.GOLD TO ROLE LEDGERLINE_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA LEDGERLINE.GOLD TO ROLE LEDGERLINE_ANALYST;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA LEDGERLINE.GOLD TO ROLE LEDGERLINE_ANALYST;

-- Check (read-only). Click each line in Query History to see its result.
SHOW SCHEMAS IN DATABASE LEDGERLINE;              -- STAGING, SNAPSHOTS, GOLD: options = MANAGED ACCESS
SHOW GRANTS TO ROLE LEDGERLINE_TRANSFORMER;
SHOW FUTURE GRANTS IN SCHEMA LEDGERLINE.GOLD;
