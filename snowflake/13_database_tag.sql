-- ledgerline — Snowflake: the mask covers the whole LEDGERLINE database (Session 19)
--
-- !! DO NOT RE-RUN. Run once 2026-10-21; it broke every read of Gold's row-policy objects and was
-- !! reverted by repairs/2026-10-21_revert_database_tag.sql (incidents.md 2026-10-21): tagging
-- !! GOVERNANCE 'public' still ATTACHES the mask to STATE_ACCESS, which the row policy reads.
-- !! Kept as the record of what ran. A working design is proven in a scratch database first.
--
-- Run in a Snowsight worksheet with the role picker on ACCOUNTADMIN, everything selected
-- (Ctrl+A, then Ctrl+Enter). Safe to re-run. decisions.md, Session 19 "The mask covers the whole
-- LEDGERLINE database"; the incident it prevents: incidents.md 2026-10-19 "Masked Gold data escaped".
--
-- Before: PII_CLASS = 'restricted' on schema GOLD only, so a copy of Gold made anywhere else in the
-- database (a scratch schema) was clear text for any reader granted it.
-- After: 'restricted' on the database, so every text column in every schema, scratch included and
-- schemas not yet created, is '***' for readers unless a nearer tag says 'public'.
-- A tag set on a schema overrides the database's; a column's own tag overrides both.
--
-- ORDER MATTERS: the two 'public' schemas are tagged BEFORE the database, so there is no moment in
-- which the loader's checks or the row policy's mapping table are masked.
--   GOVERNANCE  STATE_ROWS reads STATE_ACCESS.role_name / customer_state. Masked, the policy's
--               lookup matches nothing and every reader sees 0 rows.
--   RAW         the loader reads TABLE_NAME / SOURCE_FILE (12_check_raw.sql, pipeline_step.py
--               raw-has). Masked, check A flags every row as an unknown table and the DAG fails.
--               RAW's data is the RECORD VARIANT, which the STRING mask never covers anyway.

USE ROLE LEDGERLINE_GOVERNOR;
USE WAREHOUSE LEDGERLINE_WH;

-- 1. The two schemas that must stay readable by the roles that use them.
ALTER SCHEMA LEDGERLINE.GOVERNANCE SET TAG LEDGERLINE.GOVERNANCE.PII_CLASS = 'public';
ALTER SCHEMA LEDGERLINE.RAW        SET TAG LEDGERLINE.GOVERNANCE.PII_CLASS = 'public';

-- 2. Fail closed everywhere else.
ALTER DATABASE LEDGERLINE SET TAG LEDGERLINE.GOVERNANCE.PII_CLASS = 'restricted';

-- 3. Check (read-only). Click each line in Query History to see its result.
SELECT SYSTEM$GET_TAG('LEDGERLINE.GOVERNANCE.PII_CLASS', 'LEDGERLINE', 'database');              -- restricted
SELECT SYSTEM$GET_TAG('LEDGERLINE.GOVERNANCE.PII_CLASS', 'LEDGERLINE.GOLD', 'schema');           -- restricted
SELECT SYSTEM$GET_TAG('LEDGERLINE.GOVERNANCE.PII_CLASS', 'LEDGERLINE.GOVERNANCE', 'schema');     -- public
SELECT SYSTEM$GET_TAG('LEDGERLINE.GOVERNANCE.PII_CLASS', 'LEDGERLINE.RAW', 'schema');            -- public
-- The row policy still finds its mapping: the SP analyst sees SP's customers, not 0 rows.
USE ROLE LEDGERLINE_ANALYST_SP;
SELECT COUNT(*) FROM LEDGERLINE.GOLD.CUSTOMER_CURRENT;                                          -- 40143 (SP only)
