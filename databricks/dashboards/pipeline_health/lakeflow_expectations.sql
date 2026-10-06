-- The Lakeflow pipeline's own expectation counts, per update, table and rule (event_log of the
-- pipeline that owns the table — the whole pipeline's log, not only that table's).
WITH progress AS (
  SELECT origin.update_id AS update_id, timestamp,
         from_json(details:flow_progress.data_quality.expectations,
                   'array<struct<name: string, dataset: string, passed_records: bigint, failed_records: bigint>>')
           AS checks
  FROM event_log(TABLE(workspace.silver_dlt.inventory_cdc_checked))
  WHERE event_type = 'flow_progress'
    AND details:flow_progress.data_quality.expectations IS NOT NULL
)
SELECT min(timestamp) AS update_started, update_id, c.dataset, c.name AS expectation,
       sum(c.passed_records) AS passed, sum(c.failed_records) AS failed
FROM progress LATERAL VIEW explode(checks) t AS c
GROUP BY update_id, c.dataset, c.name
ORDER BY update_started DESC, c.dataset, c.name
