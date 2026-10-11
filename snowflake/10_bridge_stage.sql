-- ledgerline — Snowflake: the stage over the bridge bucket (Session 13)
--
-- Run from the laptop as the loader:  python scripts/sf.py --file snowflake/10_bridge_stage.sql
--
-- A FILE FORMAT says how to parse files; an EXTERNAL STAGE is a named pointer to an S3 folder,
-- reached through the storage integration (no keys). Both live in RAW, owned by LEDGERLINE_LOADER.

USE ROLE LEDGERLINE_LOADER;
USE SCHEMA LEDGERLINE.RAW;

CREATE FILE FORMAT IF NOT EXISTS PARQUET_FF
  TYPE = PARQUET
  COMMENT = 'Bridge files: Parquet written by Databricks serverless';

CREATE STAGE IF NOT EXISTS BRIDGE_SILVER
  URL = 's3://ledgerline-bridge-8176114c/silver/'
  STORAGE_INTEGRATION = LEDGERLINE_BRIDGE_S3
  FILE_FORMAT = (FORMAT_NAME = 'LEDGERLINE.RAW.PARQUET_FF')  -- qualified: an unqualified name resolves in the CALLER's schema (S13)
  COMMENT = 'Databricks Silver change-feed exports, one folder per table per version range';

-- Snowflake assumes the IAM role here for the first time: an error now is the trust policy.
LIST @BRIDGE_SILVER;
