{#- Session 17 experiment (exp_06), scratch schema S17: Silver's real order changes, replayed as if
    the bridge had delivered them in three files instead of one. Built whole on each step from
    `exp17_step`; each step's log holds every file delivered up to it.

      delivery 1  versions 2..8    on time
      delivery 2  versions 11..12  on time — the facts' high-water mark becomes 12
      delivery 3  version 9        LATE: below the mark (a Snowpipe file skipped, then reloaded)

    `arrived_at` stands in for RAW's LOADED_AT (all 118,028 rows really arrived on 10-11 in one file).
    Built only by snowflake/experiments/s17_late.py. -#}
{{ config(enabled=var('exp17', false), schema='s17', materialized='table') }}
{%- set step = var('exp17_step', 1) | int %}
with delivered as (
    select
        order_id,
        status,
        created_at,
        commit_version,
        change_type,
        case
            when commit_version <= 8 then 1
            when commit_version >= 11 then 2
            else 3
        end as delivery
    from {{ ref('stg_orders') }}
)
select
    *,
    dateadd(hour, delivery, '2026-10-18 01:00:00'::timestamp_ntz) as arrived_at
from delivered
where delivery <= {{ step }}
