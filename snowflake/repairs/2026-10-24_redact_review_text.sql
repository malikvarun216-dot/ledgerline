-- One-off repair (decisions.md S22, "Review text is redacted in Silver"): RAW.SILVER_CHANGES holds the
-- review text as Silver held it before Silver redacted it — 7 review titles with 8-14 digit runs
-- (order numbers, and numbers that may be a phone or a CPF). Silver now writes [redacted]; its v8
-- change rows arrive redacted. This rewrites the OLDER change rows' text in place, with the same
-- pattern as databricks/silver/_merge_dims.py REDACT.
--
-- Rewritten, not deleted: every row stays (so 12_check_raw's "every file's rows all loaded" still
-- holds, and lineage is kept); only the text inside RECORD changes. Time Travel keeps the old values
-- for RAW's retention (1 day), then Fail-safe 7 days (permanent table) — "deleted" there means gone
-- after ~8 days, as S18 measured.
--
-- Run AFTER the v8 export has loaded, as the table's owner:
--   python scripts/sf.py --role LEDGERLINE_LOADER --file snowflake/repairs/2026-10-24_redact_review_text.sql

USE ROLE LEDGERLINE_LOADER;
USE WAREHOUSE LEDGERLINE_WH;

-- 1. Before: rows whose title or message still matches (expected: the 7 reviews' older change rows).
SELECT 'before' AS step,
       COUNT_IF(REGEXP_LIKE(RECORD:review_comment_title::STRING, '.*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}|[0-9]{4,5}-?[0-9]{4,}).*', 's')) AS title_rows,
       COUNT_IF(REGEXP_LIKE(RECORD:review_comment_message::STRING, '.*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}|[0-9]{4,5}-?[0-9]{4,}).*', 's')) AS message_rows
FROM LEDGERLINE.RAW.SILVER_CHANGES
WHERE TABLE_NAME = 'review';

-- 2. Rewrite the title, then the message.
UPDATE LEDGERLINE.RAW.SILVER_CHANGES
SET RECORD = OBJECT_INSERT(RECORD::OBJECT, 'review_comment_title',
        REGEXP_REPLACE(RECORD:review_comment_title::STRING, '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}|[0-9]{4,5}-?[0-9]{4,}', '[redacted]'), TRUE)
WHERE TABLE_NAME = 'review'
  AND REGEXP_LIKE(RECORD:review_comment_title::STRING, '.*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}|[0-9]{4,5}-?[0-9]{4,}).*', 's');

UPDATE LEDGERLINE.RAW.SILVER_CHANGES
SET RECORD = OBJECT_INSERT(RECORD::OBJECT, 'review_comment_message',
        REGEXP_REPLACE(RECORD:review_comment_message::STRING, '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}|[0-9]{4,5}-?[0-9]{4,}', '[redacted]'), TRUE)
WHERE TABLE_NAME = 'review'
  AND REGEXP_LIKE(RECORD:review_comment_message::STRING, '.*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}|[0-9]{4,5}-?[0-9]{4,}).*', 's');

-- 3. After: expected 0 and 0.
SELECT 'after' AS step,
       COUNT_IF(REGEXP_LIKE(RECORD:review_comment_title::STRING, '.*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}|[0-9]{4,5}-?[0-9]{4,}).*', 's')) AS title_rows,
       COUNT_IF(REGEXP_LIKE(RECORD:review_comment_message::STRING, '.*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}|[0-9]{4,5}-?[0-9]{4,}).*', 's')) AS message_rows
FROM LEDGERLINE.RAW.SILVER_CHANGES
WHERE TABLE_NAME = 'review';
