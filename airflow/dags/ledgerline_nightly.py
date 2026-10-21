"""ledgerline — one dims night, end to end, across three platforms (Session 16).

decisions.md, Session 16 "Airflow runs one DAG per dims night". A DAG run IS a night: its logical
date ({{ ds }}) is the night's dump date, and the schedule lists the landed nights' dates (every
120 days from 2016-09-04, business time) instead of the calendar. One run at a time, oldest first,
each task waiting for its own previous run (depends_on_past): night n never runs before n-1 is done
(scheduled runs; a backfill needs --max-active-runs 1 for that, see below).

    land_night  ->  silver  ->  bridge_export  ->  wait_for_raw  ->  check_raw  ->  snapshots  ->  dbt_build
    (S3, AWS)      (Databricks)  (Databricks)      (Snowflake: Snowpipe loads on its own time)   (Snowflake)

Every task runs a script from the read-only repository with the pipeline's own Python
(/opt/pipeline), exactly the command a person runs by hand — see scripts/pipeline_step.py. Every
task is safe to repeat, which is what makes retries and backfills safe:

    airflow dags unpause ledgerline_nightly
    airflow backfill create --dag-id ledgerline_nightly --from-date 2016-09-04 --to-date 2018-12-23
        --reprocess-behavior completed --max-active-runs 1    (one command, one line)

Three things Airflow 3 does with a backfill, found on the first one (S17, 8 nights in 58 min):
* the DAG's max_active_runs does NOT bind it; without --max-active-runs 1 every night starts at once
  and depends_on_past alone pipelines them (night 2's silver beside night 1's bridge_export);
* with depends_on_past, --reprocess-behavior (none / completed / failed) is required;
* a paused DAG's backfill runs stay queued: unpause first, pause again after.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.bash import BashSensor
from airflow.sdk import DAG
from airflow.timetables.events import EventsTimetable
from failure_evidence import record_failure  # the DAG folder is on Airflow's import path

FIRST_NIGHT = pendulum.datetime(2016, 9, 4, tz="UTC")
LANDED = 8  # nights 1..8 (Session 15); a new night is one more date here and one more landing
NIGHTS = [FIRST_NIGHT.add(days=120 * i) for i in range(LANDED)]
# Absolute paths: a sensor does not take a working directory the way a BashOperator does.
STEP = "$PIPELINE_PYTHON $LEDGERLINE_ROOT/scripts/pipeline_step.py"

with DAG(
    dag_id="ledgerline_nightly",
    description="one dims night: land, Silver, bridge, RAW, snapshots, Gold",
    schedule=EventsTimetable(event_dates=NIGHTS, description="dims nights, every 120 days"),
    start_date=FIRST_NIGHT,
    catchup=False,  # past nights run only when asked: a backfill, never by surprise on unpause
    max_active_runs=1,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        # Session 19c: a task that failed for good leaves one evidence file for the triage agent.
        "on_failure_callback": record_failure,
    },
    tags=["ledgerline"],
) as dag:
    land_night = BashOperator(
        task_id="land_night",
        bash_command=f"{STEP} land --night {{{{ ds }}}}",
    )
    silver = BashOperator(
        task_id="silver",
        bash_command=f"{STEP} job ledgerline-silver",
        execution_timeout=timedelta(minutes=60),
    )
    # The last line printed is the bridge's summary of every exported folder (JSON) -> XCom.
    bridge_export = BashOperator(
        task_id="bridge_export",
        bash_command=f"{STEP} job ledgerline-bridge",
        execution_timeout=timedelta(minutes=60),
    )
    # A sensor: re-checks until RAW holds every row the bridge logged. Reschedule mode gives its
    # slot back between checks. Snowpipe took 25 s in S15; 30 minutes means something is wrong.
    wait_for_raw = BashSensor(
        task_id="wait_for_raw",
        bash_command=f"{STEP} raw-has '{{{{ ti.xcom_pull(task_ids=\"bridge_export\") }}}}'",
        mode="reschedule",
        poke_interval=30,
        timeout=30 * 60,
        retries=0,
    )
    check_raw = BashOperator(
        task_id="check_raw",
        bash_command=f"{STEP} check-raw",
        retries=0,  # a data problem does not fix itself on a retry
    )
    snapshots = BashOperator(
        task_id="snapshots",
        bash_command="$PIPELINE_PYTHON $LEDGERLINE_ROOT/scripts/snapshot_nights.py --apply",
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="$PIPELINE_PYTHON $LEDGERLINE_ROOT/scripts/run_dbt.py build",
        retries=0,  # a failed test is a finding, not a flake
    )

    land_night >> silver >> bridge_export >> wait_for_raw >> check_raw >> snapshots >> dbt_build
