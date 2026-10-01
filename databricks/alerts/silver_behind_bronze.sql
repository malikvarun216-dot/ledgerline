-- Alert: Silver is behind Bronze.
-- Sources whose Bronze data landed more than 26 hours ago and is still not in Silver.
-- 0 = healthy. Trigger when sources_behind > 0.
--
-- Not "no night in 26 hours": nights here land by hand, days apart, so a calendar alarm would be red
-- every quiet day (decisions.md, Session 9 revision of the alarms entry). 26 hours = one daily job run
-- plus slack, so a night that lands just after a run does not fire before the next run has had a go.

WITH bronze_nights AS (
  SELECT 'customer' AS dimension, to_date(dump_date) AS night, min(_ingested_at) AS landed
  FROM workspace.bronze.customer GROUP BY 1, 2
  UNION ALL
  SELECT 'product', to_date(dump_date), min(_ingested_at) FROM workspace.bronze.product GROUP BY 1, 2
  UNION ALL
  SELECT 'seller', to_date(dump_date), min(_ingested_at) FROM workspace.bronze.seller GROUP BY 1, 2
),
applied AS (
  SELECT dimension, max(dump_date) AS last_applied
  FROM workspace.silver.dims_merge_log GROUP BY dimension
),
dims_behind AS (
  SELECT count(DISTINCT b.dimension) AS n
  FROM bronze_nights b LEFT JOIN applied a USING (dimension)
  WHERE (a.last_applied IS NULL OR b.night > a.last_applied)
    AND b.landed < current_timestamp() - INTERVAL 26 HOURS
),
cdc_behind AS (
  -- Silver has seen sum(bronze_rows); every Bronze row older than 26 hours must be among them.
  SELECT CASE
           WHEN (SELECT count(*) FROM workspace.bronze.inventory_cdc
                 WHERE _ingested_at < current_timestamp() - INTERVAL 26 HOURS)
              > (SELECT coalesce(sum(bronze_rows), 0) FROM workspace.silver.inventory_cdc_log)
           THEN 1 ELSE 0
         END AS n
)
SELECT (SELECT n FROM dims_behind) + (SELECT n FROM cdc_behind) AS sources_behind
