-- ledgerline — Snowflake: the account's state on the trial's last day, in one result (Session 31)
--
-- Read-only. Run in a Snowsight worksheet with the role picker on ACCOUNTADMIN, everything selected
-- (Ctrl+A, then Ctrl+Enter). One grid comes back: section | item | value. Paste it back as text.
--
-- Why one query: the trial ends ~2026-11-10, after which nothing here can be read again. This records
-- what the project spent, what is still standing (scratch schemas, reader grants on scratch, CI
-- schemas) and whether the human admin has MFA, so the open incidents can be closed on numbers.
-- ACCOUNT_USAGE views lag by up to ~3 hours; anything changed in the last few hours may not show.

USE ROLE ACCOUNTADMIN;

-- 1. Spend over the whole trial, per kind of compute (what the $400 balance is charged).
SELECT 'spend' AS section, SERVICE_TYPE AS item,
       TO_VARCHAR(ROUND(SUM(CREDITS_BILLED), 4)) AS value
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
GROUP BY SERVICE_TYPE

UNION ALL
SELECT 'spend', 'TOTAL credits billed', TO_VARCHAR(ROUND(SUM(CREDITS_BILLED), 4))
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY

UNION ALL
SELECT 'spend', 'first / last usage day', MIN(USAGE_DATE) || ' / ' || MAX(USAGE_DATE)
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY

-- 2. Every schema still standing in our two databases (production ones expected; scratch = leftovers).
UNION ALL
SELECT 'schema', CATALOG_NAME || '.' || SCHEMA_NAME,
       'owner ' || COALESCE(SCHEMA_OWNER, '-') || ', created ' || TO_VARCHAR(CREATED, 'YYYY-MM-DD')
FROM SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA
WHERE DELETED IS NULL
  AND CATALOG_NAME IN ('LEDGERLINE', 'LEDGERLINE_CI')
  AND SCHEMA_NAME <> 'INFORMATION_SCHEMA'

-- 3. Reader roles' live grants outside GOLD / GOVERNANCE (the S18 leak's route: a reader on scratch).
UNION ALL
SELECT 'reader grant', GRANTEE_NAME,
       PRIVILEGE || ' on ' || GRANTED_ON || ' ' || COALESCE(TABLE_CATALOG || '.', '')
       || COALESCE(TABLE_SCHEMA || '.', '') || NAME
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE DELETED_ON IS NULL
  AND GRANTEE_NAME IN ('LEDGERLINE_ANALYST', 'LEDGERLINE_ANALYST_SP')
  AND COALESCE(TABLE_SCHEMA, '') NOT IN ('GOLD', 'GOVERNANCE')
  AND NOT (GRANTED_ON = 'SCHEMA' AND NAME IN ('GOLD', 'GOVERNANCE'))
  AND GRANTED_ON <> 'ROLE'

-- 4. People and service users: MFA and type (the 2026-10-24 incident: ZUBA11 had none).
UNION ALL
SELECT 'user', NAME,
       'type ' || COALESCE(TYPE, 'PERSON') || ', has_mfa ' || TO_VARCHAR(HAS_MFA)
       || ', last login ' || COALESCE(TO_VARCHAR(LAST_SUCCESS_LOGIN, 'YYYY-MM-DD'), 'never')
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
WHERE DELETED_ON IS NULL

ORDER BY 1, 2;
