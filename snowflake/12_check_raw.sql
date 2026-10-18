-- ledgerline — Snowflake: checks after every load into RAW.SILVER_CHANGES (Session 13)
--
--   python scripts/sf.py --file snowflake/12_check_raw.sql
--
-- Prevention rule 1 of incidents.md 2026-10-11: a derived label is checked against its source, not
-- counted. Every query returns rows ONLY when something is wrong — an empty result is a pass.

USE ROLE LEDGERLINE_LOADER;
USE WAREHOUSE LEDGERLINE_WH;

-- A. A TABLE_NAME that is not one of the seven Silver tables.
SELECT 'unknown table name' AS problem, TABLE_NAME, COUNT(*) AS rows_
FROM LEDGERLINE.RAW.SILVER_CHANGES
WHERE TABLE_NAME IS NULL
   OR TABLE_NAME NOT IN ('customer', 'product', 'seller', 'customer_order_link', 'orders', 'order_items', 'inventory', 'inventory_events')
GROUP BY 1, 2;

-- B. The label disagrees with the file path the row came from.
SELECT 'label differs from path' AS problem, TABLE_NAME, SOURCE_FILE, COUNT(*) AS rows_
FROM LEDGERLINE.RAW.SILVER_CHANGES
WHERE TABLE_NAME IS DISTINCT FROM REGEXP_SUBSTR(SOURCE_FILE, '([^/]+)/v[0-9]{10}-[0-9]{10}/', 1, 1, 'e', 1)
GROUP BY 1, 2, 3;

-- C. One row of one file loaded twice (a pipe re-created and refreshed, or COPY + pipe).
SELECT 'row loaded twice' AS problem, SOURCE_FILE, SOURCE_ROW, COUNT(*) AS copies
FROM LEDGERLINE.RAW.SILVER_CHANGES
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
LIMIT 10;

-- D. Per file: rows in RAW vs rows Snowflake says it loaded (COPY_HISTORY keeps 14 days here).
--    Only files loaded in the last 13 days: an older file has no history left, and comparing it to
--    nothing would fail every night from day 14 on a clean RAW (S17: all 11 files loaded 10-11, the
--    check would have failed from 2026-10-25). Older files stay covered: `pipeline_step.py raw-has`
--    compares rows per folder with the bridge's export_log in Databricks, which does not expire.
WITH loaded AS (
  SELECT FILE_NAME, SUM(ROW_COUNT) AS loaded_rows
  FROM TABLE(LEDGERLINE.INFORMATION_SCHEMA.COPY_HISTORY(
         TABLE_NAME => 'LEDGERLINE.RAW.SILVER_CHANGES', START_TIME => DATEADD(day, -14, CURRENT_TIMESTAMP())))
  WHERE STATUS = 'Loaded'
  GROUP BY 1
), held AS (
  SELECT SOURCE_FILE, COUNT(*) AS held_rows FROM LEDGERLINE.RAW.SILVER_CHANGES GROUP BY 1
  HAVING MAX(LOADED_AT) >= DATEADD(day, -13, CURRENT_TIMESTAMP())
)
SELECT 'file rows differ' AS problem, h.SOURCE_FILE, h.held_rows, l.loaded_rows
FROM held h LEFT JOIN loaded l ON h.SOURCE_FILE = 'silver/' || l.FILE_NAME
WHERE l.loaded_rows IS NULL OR l.loaded_rows <> h.held_rows;

-- E. A load that did not finish cleanly (Snowpipe skips a bad file whole: ON_ERROR = SKIP_FILE).
SELECT 'file not loaded' AS problem, FILE_NAME, STATUS, FIRST_ERROR_MESSAGE
FROM TABLE(LEDGERLINE.INFORMATION_SCHEMA.COPY_HISTORY(
       TABLE_NAME => 'LEDGERLINE.RAW.SILVER_CHANGES', START_TIME => DATEADD(day, -14, CURRENT_TIMESTAMP())))
WHERE STATUS <> 'Loaded';

-- Summary (always returns rows): per table, rows and change types.
SELECT TABLE_NAME,
       COUNT(*) AS rows_,
       COUNT_IF(RECORD:_change_type::STRING = 'insert') AS inserts,
       COUNT_IF(RECORD:_change_type::STRING = 'update_postimage') AS updates,
       COUNT_IF(RECORD:_change_type::STRING = 'delete') AS deletes,
       COUNT(DISTINCT SOURCE_FILE) AS files
FROM LEDGERLINE.RAW.SILVER_CHANGES
GROUP BY 1 ORDER BY 1;
