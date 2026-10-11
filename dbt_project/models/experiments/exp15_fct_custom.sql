{#- Session 15 experiment: the same incremental fact over exp15_changes, strategy 'merge_changes' —
    the project's strategy: reads the delete flag. -#}
{{ config(
    enabled=var('exp15', false), schema='s15',
    materialized='incremental', incremental_strategy='merge_changes', unique_key='order_id',
    tmp_relation_type='table',
) }}
with newest as (
    select * from {{ ref('exp15_changes') }}
    {%- if is_incremental() %}
    where commit_version > (select coalesce(max(silver_version), -1) from {{ this }})
    {%- endif %}
    qualify row_number() over (partition by order_id order by commit_version desc) = 1
)
select order_id, status, commit_version as silver_version, change_type as silver_change_type
from newest
{%- if not is_incremental() %}
where change_type <> 'delete'
{%- endif %}
