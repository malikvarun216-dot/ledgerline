"""Session 13 experiments: how Snowflake decides what to load, and what its keys do not do.

    python snowflake/experiments/s13_loading.py

Runs from the laptop as LEDGERLINE_LOADER (scripts/sf.py's key-pair sign-in). Everything it creates
lives in the scratch schema LEDGERLINE.S13 (snowflake/05_scratch_schemas.sql); production RAW is only
READ, through its stage. Safe to re-run: each part starts by re-creating its own tables.

Each step prints what Snowflake answered next to what was PREDICTED before running it. A wrong
prediction does not stop the run — it is printed as WRONG and counted, because a wrong prediction is
the finding (Session 11's rule: write the prediction down first).

  A  COPY's load history: the same file twice is skipped; FORCE loads it again
  B  a file re-delivered under the same name: identical content, then NEW content
     (compare Auto Loader, Drill 1: a rewritten file was skipped silently, new content and all)
  C  ON_ERROR and VALIDATION_MODE on a file with one bad row
  D  a pipe and a COPY over one table: do they share a load history?
  E  keys: PRIMARY KEY is not enforced; MERGE with duplicate source rows
     (compare Delta, exp_04 A1/A2)
"""

from __future__ import annotations

import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from sf import connect  # noqa: E402

from generators._common import load_local_env  # noqa: E402

S = "LEDGERLINE.S13"
BRIDGE = "@LEDGERLINE.RAW.BRIDGE_SILVER"
SELLER_FILES = ".*seller/v.*[.]parquet"  # the one seller file: 3,370 change rows
SELLER_ROWS = 3_370

results: list[tuple[str, object, object, bool]] = []


def check(label, got, predicted):
    ok = got == predicted
    results.append((label, got, predicted, ok))
    print(f"  {'ok   ' if ok else 'WRONG'} {label}: got {got!r}, predicted {predicted!r}")


def rows(cur, sql):
    cur.execute(sql)
    names = [d[0].lower() for d in cur.description or []]
    return [dict(zip(names, r, strict=True)) for r in cur.fetchall()]


def count(cur, table):
    return rows(cur, f"SELECT COUNT(*) AS n FROM {table}")[0]["n"]


def copy_status(result):
    """COPY answers one row per file, or one row saying no file was processed."""
    return [r.get("status") for r in result]


def step(title):
    print(f"\n== {title}")


# ---------------------------------------------------------------------------------------------


def part_a(cur):
    step("A. COPY's load history (table T_A, the bridge's seller file)")
    cur.execute(f"CREATE OR REPLACE TABLE {S}.T_A (RECORD VARIANT, SOURCE_FILE STRING)")
    copy = (f"COPY INTO {S}.T_A FROM (SELECT $1, METADATA$FILENAME FROM {BRIDGE}) "
            f"PATTERN = '{SELLER_FILES}'")

    first = rows(cur, copy)
    check("A1 first COPY: status", copy_status(first), ["LOADED"])
    check("A1 rows in table", count(cur, f"{S}.T_A"), SELLER_ROWS)

    second = rows(cur, copy)
    print(f"  second COPY answered: {second}")
    check("A2 same COPY again: rows in table (file skipped by load history)", count(cur, f"{S}.T_A"),
          SELLER_ROWS)

    forced = rows(cur, copy + " FORCE = TRUE")
    check("A3 FORCE = TRUE: status", copy_status(forced), ["LOADED"])
    check("A3 rows in table (loaded twice — FORCE ignores the history)", count(cur, f"{S}.T_A"),
          2 * SELLER_ROWS)


def part_b(cur, workdir):
    step("B. a file re-delivered under the same name (internal stage, table T_B)")
    cur.execute(f"CREATE OR REPLACE FILE FORMAT {S}.CSV_FF TYPE = CSV SKIP_HEADER = 1")
    cur.execute(f"CREATE OR REPLACE STAGE {S}.REDELIVER FILE_FORMAT = {S}.CSV_FF")
    cur.execute(f"CREATE OR REPLACE TABLE {S}.T_B (SELLER_ID STRING, ZIP NUMBER, CITY STRING)")
    path = workdir / "seller_night.csv"
    copy = f"COPY INTO {S}.T_B FROM @{S}.REDELIVER FILES = ('seller_night.csv')"

    def put(text):
        path.write_text(text, encoding="utf-8")
        return rows(cur, f"PUT 'file://{path.as_posix()}' @{S}.REDELIVER "
                         "AUTO_COMPRESS = FALSE OVERWRITE = TRUE")

    night = "seller_id,zip,city\nS1,13023,campinas\nS2,1001,sao paulo\nS3,80010,curitiba\n"
    put(night)
    check("B1 first delivery: rows", (copy_status(rows(cur, copy)), count(cur, f"{S}.T_B")), (["LOADED"], 3))

    put_same = put(night)
    print(f"  PUT of identical content answered: {[(r.get('source'), r.get('status')) for r in put_same]}")
    rows(cur, copy)
    check("B2 identical content, same name: rows (skipped)", count(cur, f"{S}.T_B"), 3)

    corrected = night.replace("sao paulo", "rio de janeiro")
    put(corrected)
    listed = rows(cur, f"LIST @{S}.REDELIVER")
    print(f"  stage now holds: {[(r['name'], r['size'], r['md5']) for r in listed]}")
    again = rows(cur, copy)
    print(f"  COPY of the corrected file answered: {copy_status(again)}")
    check("B3 NEW content, same name: rows (reloaded — the checksum changed)", count(cur, f"{S}.T_B"), 6)


def part_c(cur, workdir):
    step("C. ON_ERROR / VALIDATION_MODE: 4 rows, one with text in a NUMBER column (table T_C)")
    cur.execute(f"CREATE OR REPLACE STAGE {S}.BAD FILE_FORMAT = {S}.CSV_FF")
    cur.execute(f"CREATE OR REPLACE TABLE {S}.T_C (SELLER_ID STRING, ZIP NUMBER, CITY STRING)")
    path = workdir / "one_bad_row.csv"
    path.write_text("seller_id,zip,city\nS1,13023,campinas\nS2,ABC,sao paulo\nS3,80010,curitiba\n"
                    "S4,4001,santos\n", encoding="utf-8")
    rows(cur, f"PUT 'file://{path.as_posix()}' @{S}.BAD AUTO_COMPRESS = FALSE OVERWRITE = TRUE")
    copy = f"COPY INTO {S}.T_C FROM @{S}.BAD FILES = ('one_bad_row.csv')"

    errors = rows(cur, copy + " VALIDATION_MODE = RETURN_ERRORS")
    print(f"  VALIDATION_MODE errors: {[(e.get('line'), e.get('error')) for e in errors]}")
    check("C1 VALIDATION_MODE = RETURN_ERRORS: errors returned / rows loaded",
          (len(errors), count(cur, f"{S}.T_C")), (1, 0))

    try:
        rows(cur, copy)  # ON_ERROR defaults to ABORT_STATEMENT for COPY
        aborted = "no error"
    except Exception as err:  # the refusal is the expected answer
        aborted = type(err).__name__
        print(f"  default COPY refused: {str(err).splitlines()[0][:160]}")
    check("C2 default (ABORT_STATEMENT): refused, rows loaded", (aborted, count(cur, f"{S}.T_C")),
          ("ProgrammingError", 0))

    skipped = rows(cur, copy + " ON_ERROR = SKIP_FILE")
    check("C3 ON_ERROR = SKIP_FILE: status, rows loaded",
          (copy_status(skipped), count(cur, f"{S}.T_C")), (["LOAD_FAILED"], 0))

    partial = rows(cur, copy + " ON_ERROR = CONTINUE")
    answered = [(r.get("status"), r.get("rows_loaded"), r.get("errors_seen")) for r in partial]
    print(f"  CONTINUE answered: {answered}")
    check("C4 ON_ERROR = CONTINUE: status, rows loaded", (copy_status(partial), count(cur, f"{S}.T_C")),
          (["PARTIALLY_LOADED"], 3))

    rows(cur, copy + " ON_ERROR = CONTINUE")
    check("C5 the same again: a partially loaded file counts as loaded (skipped)", count(cur, f"{S}.T_C"), 3)


def wait_for(cur, table, n, seconds=180):
    """Snowpipe loads asynchronously; poll the table, never sleep blindly for long."""
    deadline = time.monotonic() + seconds
    got = count(cur, table)
    while got < n and time.monotonic() < deadline:
        time.sleep(10)
        got = count(cur, table)
    return got


def part_d(cur):
    step("D. a pipe and a COPY over one table (tables T_D1, T_D2; pipes dropped at the end)")
    select = f"SELECT $1, METADATA$FILENAME FROM {BRIDGE}"
    for t in ("T_D1", "T_D2"):
        cur.execute(f"CREATE OR REPLACE TABLE {S}.{t} (RECORD VARIANT, SOURCE_FILE STRING)")
    try:
        # D1: pipe first, then COPY. AUTO_INGEST = TRUE because REFRESH needs it — which also means
        # the pipe listens to the bridge's S3 events while it exists, so it lives for this part only.
        cur.execute(f"CREATE OR REPLACE PIPE {S}.P_D1 AUTO_INGEST = TRUE AS "
                    f"COPY INTO {S}.T_D1 FROM ({select}) PATTERN = '{SELLER_FILES}'")
        rows(cur, f"ALTER PIPE {S}.P_D1 REFRESH")
        check("D1 pipe REFRESH loads the file", wait_for(cur, f"{S}.T_D1", SELLER_ROWS), SELLER_ROWS)
        rows(cur, f"COPY INTO {S}.T_D1 FROM ({select}) PATTERN = '{SELLER_FILES}'")
        check("D1 then COPY of the same file: rows (separate histories -> loaded again)",
              count(cur, f"{S}.T_D1"), 2 * SELLER_ROWS)

        # D2: COPY first, then the pipe — the production risk: someone 'helps' RAW with a COPY.
        rows(cur, f"COPY INTO {S}.T_D2 FROM ({select}) PATTERN = '{SELLER_FILES}'")
        cur.execute(f"CREATE OR REPLACE PIPE {S}.P_D2 AUTO_INGEST = TRUE AS "
                    f"COPY INTO {S}.T_D2 FROM ({select}) PATTERN = '{SELLER_FILES}'")
        rows(cur, f"ALTER PIPE {S}.P_D2 REFRESH")
        check("D2 COPY, then pipe REFRESH: rows (separate histories -> loaded again)",
              wait_for(cur, f"{S}.T_D2", 2 * SELLER_ROWS, seconds=120), 2 * SELLER_ROWS)
    finally:
        for p in ("P_D1", "P_D2"):
            cur.execute(f"DROP PIPE IF EXISTS {S}.{p}")
        print("  scratch pipes dropped")


def part_e(cur):
    step("E. keys (table T_E)")
    cur.execute(f"CREATE OR REPLACE TABLE {S}.T_E (ID NUMBER PRIMARY KEY, V STRING)")
    cur.execute(f"INSERT INTO {S}.T_E VALUES (1, 'a'), (1, 'b'), (2, 't')")
    check("E1 PRIMARY KEY accepts a duplicate: rows with ID 1",
          rows(cur, f"SELECT COUNT(*) AS n FROM {S}.T_E WHERE ID = 1")[0]["n"], 2)

    def merge(source):
        sql = (f"MERGE INTO {S}.T_E t USING ({source}) s ON t.ID = s.ID "
               "WHEN MATCHED THEN UPDATE SET t.V = s.V "
               "WHEN NOT MATCHED THEN INSERT (ID, V) VALUES (s.ID, s.V)")
        try:
            rows(cur, sql)
            return "ok"
        except Exception as err:  # a refusal is one of the answers being measured
            print(f"    refused: {str(err).splitlines()[0][:160]}")
            return "refused"

    check("E2 two DIFFERENT source rows for existing ID 2", merge(
        "SELECT 2 AS ID, 'x' AS V UNION ALL SELECT 2, 'y'"), "refused")
    check("E3 two IDENTICAL source rows for existing ID 2", merge(
        "SELECT 2 AS ID, 'x' AS V UNION ALL SELECT 2, 'x'"), "refused")
    check("E4 two source rows for NEW ID 3", merge(
        "SELECT 3 AS ID, 'x' AS V UNION ALL SELECT 3, 'y'"), "ok")
    check("E4 rows with ID 3 (inserted twice, no error)",
          rows(cur, f"SELECT COUNT(*) AS n FROM {S}.T_E WHERE ID = 3")[0]["n"], 2)


def main() -> int:
    load_local_env()
    conn = connect(role="LEDGERLINE_LOADER")
    try:
        cur = conn.cursor()
        cur.execute("USE WAREHOUSE LEDGERLINE_WH")
        with tempfile.TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            part_a(cur)
            part_b(cur, workdir)
            part_c(cur, workdir)
            part_d(cur)
            part_e(cur)
    finally:
        conn.close()
    wrong = [r for r in results if not r[3]]
    print(f"\n{len(results)} checks, {len(wrong)} predictions wrong")
    for label, got, predicted, _ in wrong:
        print(f"  WRONG {label}: got {got!r}, predicted {predicted!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
