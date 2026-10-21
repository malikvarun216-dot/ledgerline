"""Session 19, experiment 1: Streams & Tasks — current stock kept by a task reading a stream.

    python snowflake/experiments/s19_streams_tasks.py

A STREAM on a table records the rows changed since the stream was last consumed (an offset, not a
copy). A TASK runs SQL on a schedule or on demand; WHEN SYSTEM$STREAM_HAS_DATA skips a run with
nothing new. Together they are Snowflake's in-warehouse incremental pipeline — the Gold-side twin of
Silver's foreachBatch MERGE (decisions.md S0: Gold itself stays dbt's; this is the comparison).

The data: the 158,446 inventory CDC events in RAW (34,448 I, 123,898 U, 100 D), replayed in event
order as 3 batches into a landing table, the way Snowpipe appends files. The task MERGEs each batch
into S19.INV_CURRENT with the same three guards as Silver: dedup per sku_key (newest seq), a seq
guard, deletes in-band. A delete is a TOMBSTONE (is_deleted, seq kept), not a removed row: removed,
an older update arriving later finds no row and inserts the SKU back (exp_04 C). Answer key: GOLD.
INVENTORY_CURRENT, 34,348 live SKUs, built by dbt from Silver's own state — a different path.

A stream mechanics: reading does not consume; a rolled-back transaction does not; a DML does
B the task: batch 1 by EXECUTE TASK, batch 2 by the 1-minute schedule, a scheduled run with
  nothing new is SKIPPED, batch 3, a replayed batch changes nothing; a child task runs after each
C standard vs append-only on the landing table: an UPDATE and a DELETE (a retention cleanup)
D a serverless task (no warehouse): runs, and resource monitors cannot see it

Predictions were written before the first run; a wrong one does not stop the run — it is the
finding. Every task is SUSPENDED in finally:, and SHOW TASKS is checked. Runs as
LEDGERLINE_TRANSFORMER; writes only in LEDGERLINE.S19.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from sf import connect  # noqa: E402

from generators._common import load_local_env  # noqa: E402

S = "LEDGERLINE.S19"
EVENTS, SKUS, DELETED = 158_446, 34_448, 100
LIVE = SKUS - DELETED  # 34,348 = GOLD.INVENTORY_CURRENT

results: list[tuple[str, object, object, bool]] = []


def check(label, got, predicted):
    ok = got == predicted
    results.append((label, got, predicted, ok))
    print(f"  {'ok   ' if ok else 'WRONG'} {label}: got {got!r}, predicted {predicted!r}")


def one(cur, sql: str):
    cur.execute(sql)
    return cur.fetchone()[0]


APPLY_SQL = f"""
MERGE INTO {S}.INV_CURRENT t
USING (
    SELECT event_id, sku_key, product_id, seller_id, op, seq, stock_qty, event_ts
    FROM {S}.INV_STREAM
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sku_key ORDER BY seq DESC) = 1
) s
ON t.sku_key = s.sku_key
WHEN MATCHED AND s.seq > t.seq THEN UPDATE SET
    product_id = s.product_id, seller_id = s.seller_id, stock_qty = s.stock_qty, seq = s.seq,
    event_ts = s.event_ts, is_deleted = (s.op = 'D'), applied_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
    (sku_key, product_id, seller_id, stock_qty, seq, event_ts, is_deleted, applied_at)
    VALUES (s.sku_key, s.product_id, s.seller_id, s.stock_qty, s.seq, s.event_ts, s.op = 'D',
            CURRENT_TIMESTAMP())
"""


def wait_run(cur, task: str, since: str, want: str = "SUCCEEDED", timeout: int = 240) -> str:
    """The newest completed run of `task` scheduled after `since`: its state, or 'TIMEOUT'."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        cur.execute(f"""SELECT state FROM TABLE(LEDGERLINE.INFORMATION_SCHEMA.TASK_HISTORY(
            TASK_NAME => '{task}', SCHEDULED_TIME_RANGE_START => '{since}'::TIMESTAMP_LTZ))
            WHERE state IN ('SUCCEEDED', 'FAILED', 'SKIPPED', 'CANCELLED')
              AND database_name = 'LEDGERLINE' AND schema_name = 'S19'
            ORDER BY completed_time DESC NULLS LAST, scheduled_time DESC LIMIT 1""")
        row = cur.fetchone()
        if row and (row[0] == want or row[0] in ("FAILED", "CANCELLED")):
            return row[0]
        time.sleep(5)
    return "TIMEOUT"


def now(cur) -> str:
    return str(one(cur, "SELECT CURRENT_TIMESTAMP()"))


def expected_live(cur, upto: int) -> int:
    """Answer key from the events themselves: SKUs whose newest event (batches <= upto) is not D."""
    return one(
        cur,
        f"""SELECT COUNT_IF(op <> 'D') FROM (SELECT op FROM {S}.INV_EVENTS_ALL
        WHERE batch_no <= {upto} QUALIFY ROW_NUMBER() OVER (PARTITION BY sku_key ORDER BY seq DESC) = 1)""",
    )


def land(cur, batch: int) -> int:
    cur.execute(f"""INSERT INTO {S}.INV_LANDING
        SELECT event_id, sku_key, product_id, seller_id, op, seq, stock_qty, event_ts, batch_no
        FROM {S}.INV_EVENTS_ALL WHERE batch_no = {batch}""")
    return cur.rowcount


def live(cur) -> int:
    return one(cur, f"SELECT COUNT_IF(NOT is_deleted) FROM {S}.INV_CURRENT")


def main() -> int:
    load_local_env()
    conn = connect(role="LEDGERLINE_TRANSFORMER")
    c = conn.cursor()
    tasks = [
        "APPLY_INV",
        "APPLY_INV_CHECK",
        "SERVERLESS_PING",
    ]  # root first: a child cannot be altered under a started root
    try:
        c.execute("USE WAREHOUSE LEDGERLINE_WH")
        print("\nsetup: the events, typed, numbered into 3 batches in event order")
        c.execute(f"""CREATE OR REPLACE TABLE {S}.INV_EVENTS_ALL AS
            SELECT record:event_id::string event_id, record:sku_key::string sku_key,
                   record:product_id::string product_id, record:seller_id::string seller_id,
                   record:op::string op, record:seq::int seq, record:stock_qty::int stock_qty,
                   record:event_ts::timestamp_ntz event_ts,
                   NTILE(3) OVER (ORDER BY record:event_ts::timestamp_ntz, record:seq::int) batch_no
            FROM LEDGERLINE.RAW.SILVER_CHANGES WHERE table_name = 'inventory_events'""")
        check("events", one(c, f"SELECT COUNT(*) FROM {S}.INV_EVENTS_ALL"), EVENTS)
        check(
            "distinct event ids", one(c, f"SELECT COUNT(DISTINCT event_id) FROM {S}.INV_EVENTS_ALL"), EVENTS
        )
        c.execute(f"""CREATE OR REPLACE TABLE {S}.INV_LANDING (event_id STRING, sku_key STRING,
            product_id STRING, seller_id STRING, op STRING, seq INT, stock_qty INT,
            event_ts TIMESTAMP_NTZ, batch_no INT)""")
        c.execute(f"""CREATE OR REPLACE TABLE {S}.INV_CURRENT (sku_key STRING, product_id STRING,
            seller_id STRING, stock_qty INT, seq INT, event_ts TIMESTAMP_NTZ, is_deleted BOOLEAN,
            applied_at TIMESTAMP_LTZ)""")
        c.execute(
            f"CREATE OR REPLACE TABLE {S}.INV_CHECKS (checked_at TIMESTAMP_LTZ, live INT, tombstones INT)"
        )
        c.execute(f"CREATE OR REPLACE TABLE {S}.INV_SINK LIKE {S}.INV_LANDING")
        c.execute(f"CREATE OR REPLACE STREAM {S}.INV_STREAM ON TABLE {S}.INV_LANDING APPEND_ONLY = TRUE")
        c.execute(f"""CREATE OR REPLACE TASK {S}.APPLY_INV WAREHOUSE = LEDGERLINE_WH SCHEDULE = '1 MINUTE'
            WHEN SYSTEM$STREAM_HAS_DATA('{S}.INV_STREAM') AS {APPLY_SQL}""")
        c.execute(f"""CREATE OR REPLACE TASK {S}.APPLY_INV_CHECK WAREHOUSE = LEDGERLINE_WH
            AFTER {S}.APPLY_INV AS INSERT INTO {S}.INV_CHECKS
            SELECT CURRENT_TIMESTAMP(), COUNT_IF(NOT is_deleted), COUNT_IF(is_deleted)
            FROM {S}.INV_CURRENT""")
        c.execute(f"ALTER TASK {S}.APPLY_INV_CHECK RESUME")  # a child must be resumed to run in the graph
        sizes = {
            b: one(c, f"SELECT COUNT(*) FROM {S}.INV_EVENTS_ALL WHERE batch_no = {b}") for b in (1, 2, 3)
        }
        print(f"  batch sizes {sizes}")

        print("\nA. stream mechanics (batch 1 landed, task not run yet)")
        check("A1 rows landed", land(c, 1), sizes[1])
        check("A1 stream rows", one(c, f"SELECT COUNT(*) FROM {S}.INV_STREAM"), sizes[1])
        check(
            "A2 read again: a SELECT does not consume",
            one(c, f"SELECT COUNT(*) FROM {S}.INV_STREAM"),
            sizes[1],
        )
        c.execute("BEGIN")
        c.execute(f"""INSERT INTO {S}.INV_SINK SELECT event_id, sku_key, product_id, seller_id, op, seq,
            stock_qty, event_ts, batch_no FROM {S}.INV_STREAM""")
        c.execute("ROLLBACK")
        check(
            "A3 after a rolled-back DML: not consumed",
            one(c, f"SELECT COUNT(*) FROM {S}.INV_STREAM"),
            sizes[1],
        )
        check("A3 stream has data", one(c, f"SELECT SYSTEM$STREAM_HAS_DATA('{S}.INV_STREAM')"), True)

        print("\nB1. batch 1 applied by EXECUTE TASK (the task itself is still suspended)")
        since = now(c)
        c.execute(f"EXECUTE TASK {S}.APPLY_INV")
        check("B1 run state", wait_run(c, "APPLY_INV", since), "SUCCEEDED")
        check("B1 stream consumed by the task's MERGE", one(c, f"SELECT COUNT(*) FROM {S}.INV_STREAM"), 0)
        check("B1 live SKUs = answer key", live(c), expected_live(c, 1))
        check("B1 child check ran after the root", wait_run(c, "APPLY_INV_CHECK", since), "SUCCEEDED")

        print("\nB2. batch 2 applied by the 1-minute schedule")
        c.execute(f"ALTER TASK {S}.APPLY_INV RESUME")
        since = now(c)
        check("B2 rows landed", land(c, 2), sizes[2])
        check("B2 scheduled run state", wait_run(c, "APPLY_INV", since), "SUCCEEDED")
        check("B2 live SKUs = answer key", live(c), expected_live(c, 2))

        print("\nB3. a scheduled run with nothing new: skipped, no warehouse started for it")
        since = now(c)
        check(
            "B3 scheduled run with an empty stream",
            wait_run(c, "APPLY_INV", since, want="SKIPPED", timeout=150),
            "SKIPPED",
        )
        c.execute(f"ALTER TASK {S}.APPLY_INV SUSPEND")

        print("\nB4. batch 3, then the whole of batch 2 delivered again")
        land(c, 3)
        since = now(c)
        c.execute(f"EXECUTE TASK {S}.APPLY_INV")
        check("B4 batch 3 run", wait_run(c, "APPLY_INV", since), "SUCCEEDED")
        check("B4 live SKUs = 34,348", live(c), LIVE)
        check("B4 tombstones = 100", one(c, f"SELECT COUNT_IF(is_deleted) FROM {S}.INV_CURRENT"), DELETED)
        check(
            "B4 differs from GOLD.INVENTORY_CURRENT (sku, stock, seq)",
            one(
                c,
                f"""SELECT COUNT(*) FROM
            (SELECT sku_key, stock_qty, seq FROM {S}.INV_CURRENT WHERE NOT is_deleted) t
            FULL OUTER JOIN LEDGERLINE.GOLD.INVENTORY_CURRENT g ON t.sku_key = g.sku_key
            WHERE t.sku_key IS NULL OR g.sku_key IS NULL OR t.stock_qty IS DISTINCT FROM g.stock_qty
               OR t.seq IS DISTINCT FROM g.seq""",
            ),
            0,
        )
        before = one(c, f"SELECT HASH_AGG(sku_key, stock_qty, seq, is_deleted) FROM {S}.INV_CURRENT")
        land(c, 2)  # a redelivered file: every event again
        since = now(c)
        c.execute(f"EXECUTE TASK {S}.APPLY_INV")
        check("B4 replay run", wait_run(c, "APPLY_INV", since), "SUCCEEDED")
        check(
            "B4 replay changed nothing (seq guard)",
            one(c, f"SELECT HASH_AGG(sku_key, stock_qty, seq, is_deleted) FROM {S}.INV_CURRENT") == before,
            True,
        )
        check("B4 child after the replay", wait_run(c, "APPLY_INV_CHECK", since), "SUCCEEDED")
        check(
            "B4 child checks logged (one per root run: B1, B2, B4 x2)",
            one(c, f"SELECT COUNT(*) FROM {S}.INV_CHECKS"),
            4,
        )

        print("\nC. standard vs append-only on the landing table: an UPDATE and a cleanup DELETE")
        c.execute(f"CREATE OR REPLACE STREAM {S}.INV_STD ON TABLE {S}.INV_LANDING")  # offset = now
        c.execute(f"""UPDATE {S}.INV_LANDING SET stock_qty = -1 WHERE event_id IN
            (SELECT event_id FROM {S}.INV_LANDING WHERE batch_no = 3 ORDER BY event_id LIMIT 5)""")
        c.execute(f"""DELETE FROM {S}.INV_LANDING WHERE event_id IN
            (SELECT event_id FROM {S}.INV_LANDING WHERE batch_no = 1 ORDER BY event_id LIMIT 10)""")
        check(
            "C standard stream rows (5 updates as DELETE+INSERT pairs, 10 deletes)",
            one(c, f"SELECT COUNT(*) FROM {S}.INV_STD"),
            20,
        )
        check(
            "C standard: rows flagged ISUPDATE",
            one(c, f"SELECT COUNT_IF(METADATA$ISUPDATE) FROM {S}.INV_STD"),
            10,
        )
        check(
            "C append-only stream rows (sees inserts only)", one(c, f"SELECT COUNT(*) FROM {S}.INV_STREAM"), 0
        )

        print("\nD. a serverless task: no WAREHOUSE, Snowflake-managed compute")
        c.execute(f"""CREATE OR REPLACE TASK {S}.SERVERLESS_PING
            USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' AS
            INSERT INTO {S}.INV_CHECKS SELECT CURRENT_TIMESTAMP(), -1, -1""")
        since = now(c)
        c.execute(f"EXECUTE TASK {S}.SERVERLESS_PING")
        check("D serverless run", wait_run(c, "SERVERLESS_PING", since), "SUCCEEDED")
        check(
            "D serverless successes in task history",
            one(
                c,
                """SELECT COUNT(*) FROM TABLE(
            LEDGERLINE.INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'SERVERLESS_PING'))
            WHERE state = 'SUCCEEDED'""",
            ),
            1,
        )
    finally:
        for t in tasks:
            try:
                c.execute(f"ALTER TASK IF EXISTS {S}.{t} SUSPEND")
            except Exception as e:  # a suspend that fails must be seen, not swallowed
                print(f"  SUSPEND {t} failed: {str(e).splitlines()[0][:160]}")
        c.execute(f"SHOW TASKS IN SCHEMA {S}")
        cols = [d[0].lower() for d in c.description]
        states = {r[cols.index("name")]: r[cols.index("state")] for r in c.fetchall()}
        print(f"\n  tasks after the run: {states}")
        check("every task suspended", all(v == "suspended" for v in states.values()), True)
        conn.close()
    wrong = [r for r in results if not r[3]]
    print(f"\n{len(results) - len(wrong)}/{len(results)} as predicted")
    for label, got, predicted, _ in wrong:
        print(f"  WRONG {label}: got {got!r}, predicted {predicted!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
