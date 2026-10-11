{#- Session 15 experiment, scratch schema S15: a tiny change log, the shape of stg_orders, built
    whole on each step from `exp15_step`. Each step's log holds everything delivered up to it.
    Built only by snowflake/experiments/s15_merge.py (--vars "{exp15: true, exp15_step: n}"). -#}
{{ config(enabled=var('exp15', false), schema='s15', materialized='table') }}
{%- set step = var('exp15_step', 1) | int %}
select column1::string as order_id, column2::string as status,
       column3::number(38, 0) as commit_version, column4::string as change_type
from values
    ('O1', 'created', 3, 'insert'),
    ('O2', 'created', 2, 'insert'),
    ('O3', 'created', 4, 'insert')
{%- if step >= 2 %},
    -- step 2: O1 ships, O2 is deleted, and a delete arrives for O4, an order never loaded
    ('O1', 'shipped', 7, 'update_postimage'),
    ('O2', 'created', 9, 'delete'),
    ('O4', 'created', 8, 'delete')
{%- endif %}
