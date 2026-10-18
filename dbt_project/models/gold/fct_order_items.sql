{#- One row per order item (one item = one unit sold), as Silver holds it now (Session 15).
    Same incremental shape as fct_orders: change rows that arrived after the arrival mark (minus one
    hour, S17), newest per item, minus the re-read changes the table already holds,
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
    where loaded_at > (select dateadd(hour, -1, coalesce(max(raw_loaded_at), '1970-01-01'::timestamp_ltz))
                       from {{ this }})
    {%- endif %}
),

newest as (
    select *
    from changes
    qualify row_number() over (partition by order_id, order_item_id order by commit_version desc) = 1
)

select
    o.order_id,
    o.order_item_id,
    o.product_id,
    o.seller_id,
    o.sku_key,
    o.price::number(12, 2)          as price,
    o.freight_value::number(12, 2)  as freight_value,
    o.commit_version as silver_version,
    o.change_type    as silver_change_type,
    o.loaded_at      as raw_loaded_at
from newest as o
{%- if is_incremental() %}
left join {{ this }} as f
    on f.order_id = o.order_id and f.order_item_id = o.order_item_id
-- only a change that would do something: a new item or a newer version
where f.order_id is null
   or o.commit_version > f.silver_version
{%- else %}
where o.change_type <> 'delete'
{%- endif %}
