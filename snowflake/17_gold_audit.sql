-- ledgerline — Snowflake: the schema each night's Gold is built and tested in before it is published (Session 25)
--
-- Run in a Snowsight worksheet, role picker on ACCOUNTADMIN, Ctrl+A, run. decisions.md, Session 25,
-- "Gold is published by write-audit-publish".
--
-- Why: `dbt build` replaces a Gold table first and tests it after, so a night that fails a test was
-- already readable (Drill 3, B5: 4 tables). scripts/publish_gold.py now builds the night in
-- GOLD_AUDIT, runs every test there, and clones the tables into GOLD only when all pass.
--
-- The schema is made like GOLD (06_gold_schemas.sql): owned by SYSADMIN, MANAGED ACCESS, the
-- transformer may create tables and views in it. Unlike GOLD, NO reader role gets anything here:
-- an audit night may be a failed one. Tagged 'restricted' like GOLD, so even a grant made by mistake
-- would show '***' in every text column nobody tagged public (the S18b lesson: a mask protects a place).

-- 1. The schema. SYSADMIN owns it, as it owns STAGING / SNAPSHOTS / GOLD.
USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS LEDGERLINE.GOLD_AUDIT WITH MANAGED ACCESS
  COMMENT = 'Gold write-audit-publish: tonight''s build, tested here; published to GOLD only if green';

-- 2. The transformer builds in it. Nothing for ANALYST / ANALYST_SP / the BI and copilot users.
USE ROLE SECURITYADMIN;
GRANT USAGE ON SCHEMA LEDGERLINE.GOLD_AUDIT TO ROLE LEDGERLINE_TRANSFORMER;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA LEDGERLINE.GOLD_AUDIT TO ROLE LEDGERLINE_TRANSFORMER;

-- 3. Fail closed, as GOLD (09_governance.sql).
USE ROLE LEDGERLINE_GOVERNOR;
ALTER SCHEMA LEDGERLINE.GOLD_AUDIT SET TAG LEDGERLINE.GOVERNANCE.PII_CLASS = 'restricted';

-- 4. Look. Expected: GOLD_AUDIT with options MANAGED ACCESS; the transformer's two grants and USAGE;
--    no row for any reader role; the tag 'restricted'.
USE ROLE ACCOUNTADMIN;
SHOW SCHEMAS LIKE 'GOLD_AUDIT' IN DATABASE LEDGERLINE;
SHOW GRANTS ON SCHEMA LEDGERLINE.GOLD_AUDIT;
SELECT SYSTEM$GET_TAG('LEDGERLINE.GOVERNANCE.PII_CLASS', 'LEDGERLINE.GOLD_AUDIT', 'schema') AS gold_audit_tag;
