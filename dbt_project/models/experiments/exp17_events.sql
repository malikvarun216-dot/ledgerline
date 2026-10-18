{#- Session 17 experiment (exp_06 D): a tiny event log for dbt's microbatch strategy. `event_time`
    tells a microbatch model downstream which column to filter each daily batch on.
    Dates are relative to TODAY: a normal microbatch run picks its days from the current time
    (today's batch plus `lookback` days before it), not from a date we pass.
      step 1  two events a day, today-4 .. today-1
      step 2  two LATE events: one for yesterday (one day back), one for today-4 (four days back)
    Built only by snowflake/experiments/s17_late.py. -#}
{{ config(enabled=var('exp17', false), schema='s17', materialized='table', event_time='event_at') }}
{%- set step = var('exp17_step', 1) | int %}
select column1::string as event_id,
       dateadd(hour, column3, dateadd(day, -column2, current_date())::timestamp_ntz) as event_at
from values
    ('e1', 4, 9), ('e2', 4, 17),
    ('e3', 3, 9), ('e4', 3, 17),
    ('e5', 2, 9), ('e6', 2, 17),
    ('e7', 1, 9), ('e8', 1, 17)
{%- if step >= 2 %},
    ('late1', 1, 20),
    ('late4', 4, 20)
{%- endif %}
