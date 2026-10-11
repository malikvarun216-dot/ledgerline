-- ledgerline — Snowflake: the RAW landing table and the Snowpipe that fills it (Session 13)
--
-- Run from the laptop as the loader:  python scripts/sf.py --file snowflake/11_raw_landing.sql
-- (decisions.md, Session 13 "RAW lands every bridge file in one VARIANT table, loaded only by Snowpipe")

USE ROLE LEDGERLINE_LOADER;
USE SCHEMA LEDGERLINE.RAW;

-- Each Parquet row whole (RECORD), the Silver table it belongs to, and where it came from.
CREATE TABLE IF NOT EXISTS SILVER_CHANGES (
  TABLE_NAME         STRING        COMMENT 'Silver table: the folder before the v<from>-<to> folder',
  RECORD             VARIANT       COMMENT 'the whole Parquet row, untyped; typed in Gold staging (dbt)',
  SOURCE_FILE        STRING        COMMENT 'path relative to the stage: <table>/v<from>-<to>/part-….parquet',
  SOURCE_ROW         NUMBER        COMMENT 'row number inside that file',
  FILE_LAST_MODIFIED TIMESTAMP_NTZ COMMENT 'when the file was written to S3',
  LOADED_AT          TIMESTAMP_LTZ COMMENT 'when the load started'
)
COMMENT = 'Databricks Silver change feed, as delivered. Loaded only by SILVER_CHANGES_PIPE.';

-- Snowpipe: the same COPY, run by Snowflake's serverless loader whenever S3 announces a file.
-- Names inside the pipe are fully qualified: the first version used SILVER_CHANGES / @BRIDGE_SILVER
-- and ALTER PIPE ... REFRESH failed "Table 'SILVER_CHANGES' does not exist" — the COPY is compiled
-- in the session that runs it, not where the pipe lives (Session 13).
-- NEVER `CREATE OR REPLACE` this pipe once it has loaded a file: the pipe's load history lives in the
-- pipe, and a new pipe would load every file again (decisions.md, Session 13).
CREATE PIPE IF NOT EXISTS SILVER_CHANGES_PIPE
  AUTO_INGEST = TRUE
  COMMENT = 'Loads bridge Parquet files into RAW.SILVER_CHANGES on S3 event notifications'
AS
COPY INTO LEDGERLINE.RAW.SILVER_CHANGES (TABLE_NAME, RECORD, SOURCE_FILE, SOURCE_ROW, FILE_LAST_MODIFIED, LOADED_AT)
FROM (
  -- The Silver table is the folder just before the version folder. Not "the first folder":
  -- METADATA$FILENAME starts at the bucket (silver/orders/v.../part...), not at the stage — the first
  -- load tagged all 504,039 rows 'silver' (incidents.md, 2026-10-11).
  SELECT REGEXP_SUBSTR(METADATA$FILENAME, '([^/]+)/v[0-9]{10}-[0-9]{10}/', 1, 1, 'e', 1),
         $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER,
         METADATA$FILE_LAST_MODIFIED, METADATA$START_SCAN_TIME
  FROM @LEDGERLINE.RAW.BRIDGE_SILVER
)
PATTERN = '.*[.]parquet';

-- notification_channel = the SQS queue S3 must send "file created" events to.
SHOW PIPES LIKE 'SILVER_CHANGES_PIPE';
