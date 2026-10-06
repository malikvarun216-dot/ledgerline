-- Kafka Bronze, per micro-batch: messages covered (from the offsets, not numInputRows — wrong under
-- foreachBatch on serverless) and how far behind the topic's end the stream was after it.
-- Production streams only: exp_01 records its own runs (`exp01.bug`, `exp01.fix`) in this same table —
-- experiment output inside a production table, moved out at Drill 3 with the rest (decisions.md, S11).
SELECT stream, batch_id, batch_started_at, rows_by_offsets, backlog_after_batch, recorded_at
FROM workspace.bronze.stream_progress
WHERE stream LIKE 'bronze.%'
ORDER BY recorded_at DESC
LIMIT 50
