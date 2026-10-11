-- ledgerline — Snowflake: the storage integration that lets Snowflake read the bridge bucket (Session 13)
--
-- Run in a Snowsight worksheet, role picker on ACCOUNTADMIN (creating an integration is an
-- account-level privilege), everything selected.
--
-- A STORAGE INTEGRATION is Snowflake's version of a Unity Catalog storage credential: it names one
-- IAM role in OUR AWS account that Snowflake will assume, and the S3 paths it may be used for. No AWS
-- key is ever stored in Snowflake. Snowflake assumes the role as its own AWS identity (an IAM user it
-- owns), proving which customer it acts for with an external ID — both shown by DESC INTEGRATION and
-- pasted into the role's trust policy (aws/bridge/snowflake_read_trust.template.json).
--
-- The role does not have to exist yet: Snowflake checks it only when a stage is first used.

USE ROLE ACCOUNTADMIN;
CREATE STORAGE INTEGRATION IF NOT EXISTS LEDGERLINE_BRIDGE_S3
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::240939827246:role/ledgerline-snowflake-bridge-read'
  STORAGE_ALLOWED_LOCATIONS = ('s3://ledgerline-bridge-8176114c/silver/')
  COMMENT = 'Read-only: Databricks Silver change-feed exports (decisions.md, Session 13)';

-- The loader builds stages on it; nobody else needs it.
GRANT USAGE ON INTEGRATION LEDGERLINE_BRIDGE_S3 TO ROLE LEDGERLINE_LOADER;

-- Read these two rows: STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID.
DESC INTEGRATION LEDGERLINE_BRIDGE_S3;
