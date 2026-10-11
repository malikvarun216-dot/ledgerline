{#- One row per order item (one item = one unit sold), as Silver holds it now (Session 15).
    Same incremental shape as fct_orders: change rows above the high-water mark, newest per item,
    merge_changes (in-band deletes). No joins: the order's time and person are on fct_orders, and
    the point-in-time view (order_items_at_sale) puts them together. -#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge_changes',
    unique_key=['order_id', 'order_item_id'],
    tmp_relation_type='table',
    on_schema_change='fail',
) }}

with changes as (
    select * from {{ ref('stg_order_items') }}
    {%- if is_incremental() %}
    where commit_version > (select coalesce(max(silver_version), -1) from {{ this }})
    {%- endif %}
),

newest as (
    select *
    from changes
    qualify row_number() over (partition by order_id, order_item_id order by commit_version desc) = 1
)

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    sku_key,
    price::number(12, 2)          as price,
    freight_value::number(12, 2)  as freight_value,
    commit_version as silver_version,
    change_type    as silver_change_type
from newest
{%- if not is_incremental() %}
where change_type <> 'delete'
{%- endif %}
