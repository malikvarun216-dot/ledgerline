{#- Session 17 experiment (exp_06): the same fact, high-water mark on ARRIVAL time (when the row
    reached RAW), not on the version. A late file arrives after the mark whatever versions it holds;
    merge_changes' `>=` version guard still decides which change wins. -#}
{{ config(
    enabled=var('exp17', false), schema='s17',
    materialized='incremental', incremental_strategy='merge_changes', unique_key='order_id',
    tmp_relation_type='table',
) }}
with newest as (
    select * from {{ ref('exp17_changes') }}
    {%- if is_incremental() %}
    where arrived_at > (select coalesce(max(arrived_at), '1970-01-01'::timestamp_ntz) from {{ this }})
    {%- endif %}
    qualify row_number() over (partition by order_id order by commit_version desc) = 1
)
select order_id, status, created_at, commit_version as silver_version, change_type as silver_change_type,
       arrived_at
from newest
{%- if not is_incremental() %}
where change_type <> 'delete'
{%- endif %}
