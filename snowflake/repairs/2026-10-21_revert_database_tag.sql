-- ledgerline — repair 2026-10-21: undo 13_database_tag.sql (incidents.md 2026-10-21, Session 19)
--
-- Run in Snowsight with the role picker on ACCOUNTADMIN, everything selected (Ctrl+A, Ctrl+Enter).
--
-- What broke: once the GOVERNANCE schema carried the PII_CLASS tag (any value, 'public' included),
-- the tag's masking policy became ATTACHED to STATE_ACCESS's text columns. The row policy STATE_ROWS
-- looks up STATE_ACCESS in its body, and Snowflake refuses a policy body that reads a column under
-- another policy: every read of a Gold object with the row policy fails, for every role.
-- A tag value decides what the mask RETURNS; the tag's presence decides where the mask is ATTACHED.
--
-- This returns the account to the state verified in S18: only the GOLD schema tagged 'restricted'.

USE ROLE LEDGERLINE_GOVERNOR;
USE WAREHOUSE LEDGERLINE_WH;

ALTER DATABASE LEDGERLINE          UNSET TAG LEDGERLINE.GOVERNANCE.PII_CLASS;
ALTER SCHEMA LEDGERLINE.GOVERNANCE UNSET TAG LEDGERLINE.GOVERNANCE.PII_CLASS;
ALTER SCHEMA LEDGERLINE.RAW        UNSET TAG LEDGERLINE.GOVERNANCE.PII_CLASS;

-- Check. Each result is the LAST statement only: read the others in Query History.
SELECT SYSTEM$GET_TAG('LEDGERLINE.GOVERNANCE.PII_CLASS', 'LEDGERLINE', 'database');              -- NULL
SELECT SYSTEM$GET_TAG('LEDGERLINE.GOVERNANCE.PII_CLASS', 'LEDGERLINE.GOVERNANCE', 'schema');     -- NULL
SELECT SYSTEM$GET_TAG('LEDGERLINE.GOVERNANCE.PII_CLASS', 'LEDGERLINE.GOLD', 'schema');           -- restricted (untouched)
USE ROLE LEDGERLINE_ANALYST;
SELECT COUNT(*) FROM LEDGERLINE.GOLD.CUSTOMER_CURRENT;                                          -- 95696
USE ROLE LEDGERLINE_ANALYST_SP;
SELECT COUNT(*) FROM LEDGERLINE.GOLD.CUSTOMER_CURRENT;                                          -- 40143
