{#- Session 17 experiment (exp_06 D): dbt microbatch. dbt splits the run into one batch per day of
    `event_time`, filters the upstream model to that day, and REPLACES that day's rows (on Snowflake:
    delete + insert for the day's range). `lookback=1` also re-runs the day before the run's window:
    an event up to one day late is picked up; an older one is not, until someone re-runs its days
    (`--event-time-start` / `--event-time-end`). Lateness here is in EVENT time, not arrival. -#}
{{ config(
    enabled=var('exp17', false), schema='s17',
    materialized='incremental', incremental_strategy='microbatch',
    event_time='event_day', batch_size='day', lookback=1, begin='2026-10-01',
    tmp_relation_type='table',
) }}
select date_trunc('day', event_at) as event_day, count(*) as events
from {{ ref('exp17_events') }}
group by 1
