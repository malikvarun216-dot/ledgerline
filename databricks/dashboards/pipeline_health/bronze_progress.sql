-- Kafka Bronze, per micro-batch: messages covered (from the offsets, not numInputRows — wrong under
-- foreachBatch on serverless) and how far behind the topic's end the stream was after it.
SELECT stream, batch_id, batch_started_at, rows_by_offsets, backlog_after_batch, recorded_at
FROM workspace.bronze.stream_progress
ORDER BY recorded_at DESC
LIMIT 50
