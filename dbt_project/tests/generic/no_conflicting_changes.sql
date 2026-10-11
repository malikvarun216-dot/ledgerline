{#-
  Two DIFFERENT rows for one key at one change-feed version: fails.

  The same row twice is allowed — a file loaded twice (incidents.md 2026-10-11, B70) must change
  nothing downstream, and the current-state pick below absorbs it. Two different rows at one
  (key, version) cannot come from Delta's change feed, so they mean the bridge or RAW is wrong; the
  pick would choose one of them silently. Same rule as Silver's seq tie (decisions.md, Session 9):
  a tie between different events is refused, never resolved by picking.

  "Different" = a different RECORD: staging's change_hash is hash(record), the whole Parquet row, so
  the file name and row number of a re-load do not count. (HASH(* EXCLUDE (...)) was the first
  version; Snowflake refused it at compile time, 002079.)

  Usage (one key or several):
    data_tests:
      - no_conflicting_changes:
          arguments:
            key: ['order_id', 'order_item_id']
-#}
{% test no_conflicting_changes(model, key) %}
select {{ key | join(', ') }}, commit_version,
       count(*) as copies,
       count(distinct change_hash) as different_rows
from {{ model }}
group by {{ key | join(', ') }}, commit_version
having count(distinct change_hash) > 1
{% endtest %}
