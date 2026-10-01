-- Alert: dimension rows are being held.
-- Rows held by the latest apply of each dimension: a bad value kept that key at its previous values
-- (decisions.md, Session 8 revision "hold the bad row"). The rows themselves are in
-- workspace.silver.dims_rejected_rows. 0 = healthy. Trigger when rows_held > 0.
--
-- Without this alarm a held row could stay stale silently: the job succeeds when it holds a row.

SELECT coalesce(sum(held), 0) AS rows_held
FROM (
  SELECT dimension, max_by(held, applied_at) AS held
  FROM workspace.silver.dims_merge_log
  GROUP BY dimension
)
