-- ledgerline — Snowflake: the identity the analyst copilot reads Gold with (Session 24)
--
-- Run in a Snowsight worksheet, role picker on ACCOUNTADMIN, Ctrl+A, run. Then run the ALTER USER
-- line that `python scripts/sf_keypair.py --user LEDGERLINE_COPILOT_SVC --out ~/.snowflake/ledgerline_copilot_svc.p8`
-- printed (it carries the public key). decisions.md, Session 24.
--
-- Why a user of its own (the 15_bi_reader.sql reasoning, one more time): the copilot's SQL is written
-- by a model, so the guarantee that it cannot see past a mask or change a table must not live in the
-- prompt. This user holds the two reader roles and nothing else; secondary roles off, so a session
-- has exactly one of them. Its queries are told apart from Power BI's by user in QUERY_HISTORY.
--
-- Warehouse: LEDGERLINE_BI_WH (XSMALL, 60 s, 5-min cap, daily monitor 1 credit) — already granted to
-- both reader roles by 15_bi_reader.sql. No warehouse of its own (decisions.md, Session 24).

-- 1. The user: a program, not a person — key pair only, never a password.
USE ROLE USERADMIN;
CREATE USER IF NOT EXISTS LEDGERLINE_COPILOT_SVC
  TYPE = SERVICE
  COMMENT = 'ledgerline analyst copilot (S24): key pair only, reader roles only';
ALTER USER LEDGERLINE_COPILOT_SVC SET
  DEFAULT_ROLE = LEDGERLINE_ANALYST
  DEFAULT_WAREHOUSE = LEDGERLINE_BI_WH
  DEFAULT_NAMESPACE = LEDGERLINE.GOLD
  DEFAULT_SECONDARY_ROLES = ();

USE ROLE SECURITYADMIN;
GRANT ROLE LEDGERLINE_ANALYST    TO USER LEDGERLINE_COPILOT_SVC;
GRANT ROLE LEDGERLINE_ANALYST_SP TO USER LEDGERLINE_COPILOT_SVC;

-- 2. Cortex Analyst for the two reader roles — the narrow database role (Analyst only), not
-- CORTEX_USER (every Cortex AI function). B?: if this grant is refused on the trial, the comparison
-- falls back to the Snowsight playground (decisions.md, Session 24). Database roles of the SNOWFLAKE
-- database are granted by ACCOUNTADMIN.
USE ROLE ACCOUNTADMIN;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_ANALYST_USER TO ROLE LEDGERLINE_ANALYST;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_ANALYST_USER TO ROLE LEDGERLINE_ANALYST_SP;

-- 3. Scratch schema S24: one semantic view for Cortex Analyst (copilot/semantic_view.sql, built by the
-- transformer). Kept in this file, not 05_scratch_schemas.sql, so one Ctrl+A runs the session's
-- setup. **The one scratch schema a reader may enter** — the S18 rule ("no reader grant on scratch")
-- exists because S18's scratch held a clear-text COPY of Gold. A semantic view holds no data: it is
-- read at question time, against GOLD, under the reader's own masks. So readers get USAGE here and
-- SELECT on that one view (granted by its owner), never on future tables, and the check below
-- proves S24 holds no table. S21 goes, as carried from S21.
USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS LEDGERLINE.S24
  COMMENT = 'Session 24: the semantic view Cortex Analyst answers from, beside the copilot. No tables. Scratch.';
DROP SCHEMA IF EXISTS LEDGERLINE.S21;

USE ROLE SECURITYADMIN;
GRANT USAGE ON SCHEMA LEDGERLINE.S24 TO ROLE LEDGERLINE_TRANSFORMER;
GRANT CREATE SEMANTIC VIEW ON SCHEMA LEDGERLINE.S24 TO ROLE LEDGERLINE_TRANSFORMER;
GRANT USAGE ON SCHEMA LEDGERLINE.S24 TO ROLE LEDGERLINE_ANALYST;
GRANT USAGE ON SCHEMA LEDGERLINE.S24 TO ROLE LEDGERLINE_ANALYST_SP;

-- Check: TYPE = SERVICE, HAS_PASSWORD = false, DEFAULT_SECONDARY_ROLES = [], and (after the
-- ALTER USER ... RSA_PUBLIC_KEY line) RSA_PUBLIC_KEY_FP = the fingerprint the script printed.
-- Then: exactly two role grants, both readers; the analyst role holds CORTEX_ANALYST_USER and USAGE
-- on S24; S24 has no table; no schema S21.
DESC USER LEDGERLINE_COPILOT_SVC;
SHOW GRANTS TO USER LEDGERLINE_COPILOT_SVC;
SHOW GRANTS TO ROLE LEDGERLINE_ANALYST;
SHOW GRANTS ON SCHEMA LEDGERLINE.S24;
SELECT COUNT(*) AS tables_in_s24 FROM LEDGERLINE.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'S24';   -- 0
SHOW SCHEMAS LIKE 'S2%' IN DATABASE LEDGERLINE;   -- S24 only
