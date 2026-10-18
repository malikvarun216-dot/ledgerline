{#- Session 17 experiment (exp_06): the production pattern of fct_orders — a high-water mark on the
    change-feed VERSION, read from this table — with an optional lookback of `exp17_lookback`
    versions (0 = production today). merge_changes' `>=` guard keeps an older change from
    overwriting a newer one, so re-reading versions is safe; it only costs rows. -#}
{{ config(
    enabled=var('exp17', false), schema='s17',
    materialized='incremental', incremental_strategy='merge_changes', unique_key='order_id',
    tmp_relation_type='table',
) }}
{%- set lookback = var('exp17_lookback', 0) | int %}
with newest as (
    select * from {{ ref('exp17_changes') }}
    {%- if is_incremental() %}
    where commit_version > (select coalesce(max(silver_version), -1) from {{ this }}) - {{ lookback }}
    {%- endif %}
    qualify row_number() over (partition by order_id order by commit_version desc) = 1
)
select order_id, status, created_at, commit_version as silver_version, change_type as silver_change_type
from newest
{%- if not is_incremental() %}
where change_type <> 'delete'
{%- endif %}
