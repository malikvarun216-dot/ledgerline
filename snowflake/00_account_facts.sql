-- ledgerline — Snowflake: what the account actually is (Session 13). Read-only.
-- Run in a Snowsight worksheet. The answers are recorded in CLAUDE.md and progress.md as verified.

SELECT CURRENT_ORGANIZATION_NAME() AS org,
       CURRENT_ACCOUNT_NAME()      AS account_name,
       CURRENT_ACCOUNT()           AS locator,
       CURRENT_REGION()            AS region,
       CURRENT_VERSION()           AS version,
       CURRENT_USER()              AS me,
       CURRENT_ROLE()              AS role;

SHOW WAREHOUSES;   -- what exists that nobody here created
SHOW DATABASES;

-- The edition, as the organisation sees it. On 2026-10-11 the role switch worked and this
-- returned 0 rows (finding, Session 13) — the edition is proven another way (02_roles.sql).
USE ROLE ORGADMIN;
SHOW ORGANIZATION ACCOUNTS;
USE ROLE ACCOUNTADMIN;
