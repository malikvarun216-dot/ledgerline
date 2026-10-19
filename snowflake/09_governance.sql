-- ledgerline — Snowflake: who may see what in GOLD (Session 18)
--
-- Run in a Snowsight worksheet with the role picker on ACCOUNTADMIN, everything selected
-- (Ctrl+A, then Ctrl+Enter). Safe to re-run. decisions.md, Session 18 "Who may see what on
-- Snowflake Gold"; the experiment behind it: snowflake/experiments/s18_governance.py.
--
--   LEDGERLINE_GOVERNOR     owns every tag and policy (schema LEDGERLINE.GOVERNANCE). Held by the
--                           human only. Reads no data. Rolls up to SECURITYADMIN, NOT SYSADMIN: the
--                           roles that own and build data must not decide who may read it.
--   PII_CLASS (tag)         'restricted' on the whole GOLD schema = every text column masked by
--                           default; dbt tags the safe columns 'public' after each build.
--   MASK_TEXT_EXEMPT        bound to the tag: clear for the pipeline roles and for 'public' columns,
--                           '***' otherwise. Fails closed: a column dbt forgot is hidden, not leaked.
--   STATE_ROWS + STATE_ACCESS   row access by customer_state from a mapping table (role -> state,
--                           '*' = all). Attached by dbt after each build (a row policy cannot be
--                           bound to a tag).
--   LEDGERLINE_ANALYST_SP   a regional analyst: SP customers only. Exists to test STATE_ROWS.
--
-- AFTER RUNNING: every GOLD text column shows '***' to analysts until the next dbt build tags the
-- public ones (python scripts/run_dbt.py build). Too much hidden for a few minutes, never too little.

-- 1. Roles. USERADMIN makes roles.
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS LEDGERLINE_GOVERNOR
  COMMENT = 'ledgerline: owns tags, masking and row access policies; reads no data';
CREATE ROLE IF NOT EXISTS LEDGERLINE_ANALYST_SP
  COMMENT = 'ledgerline: reads GOLD, customers in SP only (row access test role)';

GRANT ROLE LEDGERLINE_GOVERNOR   TO ROLE SECURITYADMIN;
GRANT ROLE LEDGERLINE_GOVERNOR   TO USER ZUBA11;
GRANT ROLE LEDGERLINE_ANALYST_SP TO ROLE SYSADMIN;
GRANT ROLE LEDGERLINE_ANALYST_SP TO USER ZUBA11;
-- The service user tests a reader as itself (its secondary roles are off: a session holds only the
-- role it names). A reader role, granting no write — decisions.md S18 records the exception.
GRANT ROLE LEDGERLINE_ANALYST_SP TO USER LEDGERLINE_SVC;

-- 2. The governance schema, handed to the governor. SYSADMIN creates schemas in this project.
USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS LEDGERLINE.GOVERNANCE WITH MANAGED ACCESS
  COMMENT = 'Tags, masking and row access policies, and their mapping tables. Owned by LEDGERLINE_GOVERNOR.';
GRANT OWNERSHIP ON SCHEMA LEDGERLINE.GOVERNANCE TO ROLE LEDGERLINE_GOVERNOR COPY CURRENT GRANTS;

-- 3. Privileges. SECURITYADMIN grants. The three account-level APPLY privileges are what the
--    experiment found the transformer lacked (s18_governance B1 / C1): only the governor gets them.
USE ROLE SECURITYADMIN;
GRANT USAGE ON WAREHOUSE LEDGERLINE_WH TO ROLE LEDGERLINE_GOVERNOR;
GRANT USAGE ON DATABASE LEDGERLINE     TO ROLE LEDGERLINE_GOVERNOR;
GRANT APPLY MASKING POLICY    ON ACCOUNT TO ROLE LEDGERLINE_GOVERNOR;
GRANT APPLY ROW ACCESS POLICY ON ACCOUNT TO ROLE LEDGERLINE_GOVERNOR;
GRANT APPLY TAG               ON ACCOUNT TO ROLE LEDGERLINE_GOVERNOR;

GRANT USAGE ON WAREHOUSE LEDGERLINE_WH  TO ROLE LEDGERLINE_ANALYST_SP;
GRANT USAGE ON DATABASE LEDGERLINE      TO ROLE LEDGERLINE_ANALYST_SP;
GRANT USAGE ON SCHEMA LEDGERLINE.GOLD   TO ROLE LEDGERLINE_ANALYST_SP;
GRANT SELECT ON ALL TABLES    IN SCHEMA LEDGERLINE.GOLD TO ROLE LEDGERLINE_ANALYST_SP;
GRANT SELECT ON ALL VIEWS     IN SCHEMA LEDGERLINE.GOLD TO ROLE LEDGERLINE_ANALYST_SP;
GRANT SELECT ON FUTURE TABLES IN SCHEMA LEDGERLINE.GOLD TO ROLE LEDGERLINE_ANALYST_SP;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA LEDGERLINE.GOLD TO ROLE LEDGERLINE_ANALYST_SP;

-- 4. The tag, the policies, the mapping — created by the governor, which owns them.
USE ROLE LEDGERLINE_GOVERNOR;
USE WAREHOUSE LEDGERLINE_WH;

CREATE TAG IF NOT EXISTS LEDGERLINE.GOVERNANCE.PII_CLASS
  ALLOWED_VALUES 'restricted', 'public'
  COMMENT = 'restricted = masked for readers; public = clear. Set restricted on schema GOLD; dbt sets public per column.';

-- EXEMPT_OTHER_POLICIES: the tag puts this mask on EVERY text column in GOLD, customer_state
-- included, and Snowflake refuses a masked column as a row policy argument unless the mask is
-- exempt (incidents.md 2026-10-18, error 391819). Can only be set when the policy is created.
CREATE MASKING POLICY IF NOT EXISTS LEDGERLINE.GOVERNANCE.MASK_TEXT_EXEMPT AS (val STRING) RETURNS STRING ->
  CASE
    WHEN IS_ROLE_IN_SESSION('LEDGERLINE_TRANSFORMER') OR IS_ROLE_IN_SESSION('LEDGERLINE_CI_RUNNER') THEN val
    WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('LEDGERLINE.GOVERNANCE.PII_CLASS') = 'public' THEN val
    ELSE '***'
  END
  EXEMPT_OTHER_POLICIES = TRUE
  COMMENT = 'Bound to tag PII_CLASS. Pipeline roles see clear text; readers see public columns only. A filter on a masked column matches nothing (0 rows, not an error). Exempt, so a row policy may read a column it covers.';

CREATE TABLE IF NOT EXISTS LEDGERLINE.GOVERNANCE.STATE_ACCESS (
  role_name      STRING NOT NULL,
  customer_state STRING NOT NULL   -- a two-letter state, or '*' for every state
) COMMENT = 'Row access mapping for STATE_ROWS: which role sees which customer_state.';

-- Re-runnable: replace the mapping as a whole. The pipeline roles MUST be '*': a row policy also
-- filters the owner, and dbt's incremental facts read their own table (constraints.md, S18).
DELETE FROM LEDGERLINE.GOVERNANCE.STATE_ACCESS;
INSERT INTO LEDGERLINE.GOVERNANCE.STATE_ACCESS VALUES
  ('LEDGERLINE_TRANSFORMER', '*'),
  ('LEDGERLINE_CI_RUNNER',   '*'),
  ('LEDGERLINE_ANALYST',     '*'),
  ('LEDGERLINE_ANALYST_SP',  'SP');

CREATE ROW ACCESS POLICY IF NOT EXISTS LEDGERLINE.GOVERNANCE.STATE_ROWS AS (state STRING) RETURNS BOOLEAN ->
  EXISTS (SELECT 1 FROM LEDGERLINE.GOVERNANCE.STATE_ACCESS m
          WHERE m.customer_state IN (state, '*') AND IS_ROLE_IN_SESSION(m.role_name))
  COMMENT = 'Rows by customer_state from STATE_ACCESS. A role with no mapping sees no rows.';

-- Bind the masking policy to the tag. Once only: on a re-run this line errors ("already
-- attached") and the run stops here — then select and run the lines below it instead.
ALTER TAG LEDGERLINE.GOVERNANCE.PII_CLASS SET MASKING POLICY LEDGERLINE.GOVERNANCE.MASK_TEXT_EXEMPT;

-- Fail closed: every text column in GOLD is 'restricted' unless its own column tag says otherwise.
ALTER SCHEMA LEDGERLINE.GOLD SET TAG LEDGERLINE.GOVERNANCE.PII_CLASS = 'restricted';

-- What dbt may do — and nothing account-wide: reference the schema, set THIS tag on columns of the
-- tables it owns, attach THIS row policy. CI's dbt runs the same post-hooks in LEDGERLINE_CI.
GRANT USAGE ON SCHEMA LEDGERLINE.GOVERNANCE TO ROLE LEDGERLINE_TRANSFORMER;
GRANT USAGE ON SCHEMA LEDGERLINE.GOVERNANCE TO ROLE LEDGERLINE_CI_RUNNER;
GRANT APPLY ON TAG LEDGERLINE.GOVERNANCE.PII_CLASS TO ROLE LEDGERLINE_TRANSFORMER;
GRANT APPLY ON TAG LEDGERLINE.GOVERNANCE.PII_CLASS TO ROLE LEDGERLINE_CI_RUNNER;
GRANT APPLY ON ROW ACCESS POLICY LEDGERLINE.GOVERNANCE.STATE_ROWS TO ROLE LEDGERLINE_TRANSFORMER;
GRANT APPLY ON ROW ACCESS POLICY LEDGERLINE.GOVERNANCE.STATE_ROWS TO ROLE LEDGERLINE_CI_RUNNER;

-- 5. Check (read-only). Click each line in Query History to see its result.
SHOW TAGS IN SCHEMA LEDGERLINE.GOVERNANCE;
SELECT * FROM TABLE(LEDGERLINE.INFORMATION_SCHEMA.POLICY_REFERENCES(
  POLICY_NAME => 'LEDGERLINE.GOVERNANCE.MASK_TEXT_EXEMPT'));          -- 1 row: ref_entity_domain = TAG
SELECT SYSTEM$GET_TAG('LEDGERLINE.GOVERNANCE.PII_CLASS', 'LEDGERLINE.GOLD', 'schema');   -- restricted
SELECT * FROM LEDGERLINE.GOVERNANCE.STATE_ACCESS;              -- 4 rows
SHOW GRANTS TO ROLE LEDGERLINE_GOVERNOR;
