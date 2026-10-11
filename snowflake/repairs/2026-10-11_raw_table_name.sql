-- One-off repair (incidents.md, 2026-10-11): RAW.SILVER_CHANGES.TABLE_NAME was 'silver' on all
-- 504,039 rows of the first load. Re-derived from SOURCE_FILE, which holds the full path on every row.
-- RECORD (the delivered data) is not touched. Run once:
--   python scripts/sf.py --role LEDGERLINE_LOADER --file snowflake/repairs/2026-10-11_raw_table_name.sql

USE ROLE LEDGERLINE_LOADER;
USE WAREHOUSE LEDGERLINE_WH;

-- 1. The rows: expected 504,039 updated.
UPDATE LEDGERLINE.RAW.SILVER_CHANGES
SET TABLE_NAME = REGEXP_SUBSTR(SOURCE_FILE, '([^/]+)/v[0-9]{10}-[0-9]{10}/', 1, 1, 'e', 1)
WHERE TABLE_NAME = 'silver';

-- 2. The pipe, re-created with the same expression while PAUSED, then left paused until the S3
--    notification is set up. Re-creating drops its load history: no REFRESH before 2026-10-18.
ALTER PIPE LEDGERLINE.RAW.SILVER_CHANGES_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;
CREATE OR REPLACE PIPE LEDGERLINE.RAW.SILVER_CHANGES_PIPE
  AUTO_INGEST = TRUE
  COMMENT = 'Loads bridge Parquet files into RAW.SILVER_CHANGES on S3 event notifications'
AS
COPY INTO LEDGERLINE.RAW.SILVER_CHANGES (TABLE_NAME, RECORD, SOURCE_FILE, SOURCE_ROW, FILE_LAST_MODIFIED, LOADED_AT)
FROM (
  SELECT REGEXP_SUBSTR(METADATA$FILENAME, '([^/]+)/v[0-9]{10}-[0-9]{10}/', 1, 1, 'e', 1),
         $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER,
         METADATA$FILE_LAST_MODIFIED, METADATA$START_SCAN_TIME
  FROM @LEDGERLINE.RAW.BRIDGE_SILVER
)
PATTERN = '.*[.]parquet';
ALTER PIPE LEDGERLINE.RAW.SILVER_CHANGES_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;
SELECT SYSTEM$PIPE_STATUS('LEDGERLINE.RAW.SILVER_CHANGES_PIPE') AS status;
