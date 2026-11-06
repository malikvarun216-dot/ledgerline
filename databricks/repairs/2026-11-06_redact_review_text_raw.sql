-- One-off repair (incidents.md 2026-11-06): workspace.raw.silver_changes was loaded from the bridge's
-- Parquet files, which still hold the review titles S22 redacted everywhere else (8 rows, 7 reviews).
-- This is the Databricks twin of snowflake/repairs/2026-10-24_redact_review_text.sql: same pattern,
-- the text rewritten in place, every row kept.
--
-- Databricks has no OBJECT_INSERT for a VARIANT, so the record is rebuilt field by field with the
-- types the rows carry (schema_of_variant_agg, read 2026-11-06) — a JSON round trip would turn every
-- TIMESTAMP into a string and fail raw_fields_declared.
--
-- Run as the table's owner, the ledgerline-ci service principal (scripts/dbx.py cursor), or in the
-- SQL Editor by the workspace owner. Expected: step 1 = 8, step 2 updates 8, step 3 = 0.

-- 1. Before
SELECT count(*) AS before_rows
FROM workspace.raw.silver_changes
WHERE table_name = 'review'
  AND (record:['review_comment_title']::string
         rlike '(?s)^.*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}|[0-9]{4,5}-?[0-9]{4,}).*$'
    OR record:['review_comment_message']::string
         rlike '(?s)^.*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}|[0-9]{4,5}-?[0-9]{4,}).*$');

-- 2. Rewrite title and message (a field without a match is unchanged by regexp_replace)
UPDATE workspace.raw.silver_changes
SET record = to_variant_object(named_struct(
    '_change_type',            record:['_change_type']::string,
    '_commit_timestamp',       record:['_commit_timestamp']::timestamp,
    '_commit_version',         record:['_commit_version']::bigint,
    '_first_seen_dump_date',   record:['_first_seen_dump_date']::date,
    '_last_changed_dump_date', record:['_last_changed_dump_date']::date,
    '_merged_at',              record:['_merged_at']::timestamp,
    '_source_file',            record:['_source_file']::string,
    'dim_updated_at',          record:['dim_updated_at']::timestamp,
    'order_id',                record:['order_id']::string,
    'review_answer_timestamp', record:['review_answer_timestamp']::timestamp,
    'review_comment_message',  regexp_replace(record:['review_comment_message']::string,
                                 '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}|[0-9]{4,5}-?[0-9]{4,}', '[redacted]'),
    'review_comment_title',    regexp_replace(record:['review_comment_title']::string,
                                 '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}|[0-9]{4,5}-?[0-9]{4,}', '[redacted]'),
    'review_creation_date',    record:['review_creation_date']::timestamp,
    'review_id',               record:['review_id']::string,
    'review_score',            record:['review_score']::bigint))
WHERE table_name = 'review'
  AND (record:['review_comment_title']::string
         rlike '(?s)^.*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}|[0-9]{4,5}-?[0-9]{4,}).*$'
    OR record:['review_comment_message']::string
         rlike '(?s)^.*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}|[0-9]{4,5}-?[0-9]{4,}).*$');

-- 3. After
SELECT count(*) AS after_rows
FROM workspace.raw.silver_changes
WHERE table_name = 'review'
  AND (record:['review_comment_title']::string
         rlike '(?s)^.*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}|[0-9]{4,5}-?[0-9]{4,}).*$'
    OR record:['review_comment_message']::string
         rlike '(?s)^.*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}|[0-9]{4,5}-?[0-9]{4,}).*$');
