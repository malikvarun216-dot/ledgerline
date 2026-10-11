{#- One row per order, as Silver holds it now (Session 15; decisions.md "Gold facts ...").

    INCREMENTAL: each run reads only the change rows newer than the newest version already applied
    (`silver_version`, a high-water mark taken from this table), keeps each order's newest change,
    and applies it with the merge_changes strategy (macros/merge_changes.sql): insert, update, and
    an in-band delete. The first build reads the whole log and drops deleted orders.

    LATE-ARRIVING LINK: an order's person (customer_unique_id) comes from another source, the nightly
    customer_order_link dump, which can arrive after the order. Such an order is loaded with NULL
    and SELECTED AGAIN on the first run after its link exists — its order change has not moved, so
    the high-water mark alone would never look at it again.

    Known limit, on purpose until exp_06 (S17): a change file loaded LATE with versions below the
    high-water mark is skipped for good. -#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge_changes',
    unique_key='order_id',
    tmp_relation_type='table',
    on_schema_change='fail',
) }}

with changes as (
    select * from {{ ref('stg_orders') }}
    {%- if is_incremental() %}
    where commit_version > (select coalesce(max(silver_version), -1) from {{ this }})
       -- re-try an unlinked order only once its link EXISTS: re-trying every unlinked order made
       -- each run rewrite all 99,441 of them while the link was still missing (Session 15)
       or order_id in (
           select f.order_id
           from {{ this }} as f
           join {{ ref('int_customer_order_link') }} as l on l.customer_id = f.customer_id
           where f.customer_unique_id is null
       )
    {%- endif %}
),

newest as (
    select *
    from changes
    qualify row_number() over (partition by order_id order by commit_version desc) = 1
)

select
    o.order_id,
    o.customer_id,
    l.customer_unique_id,
    o.status,
    o.source_status,
    o.created_at,
    o.approved_at,
    o.shipped_at,
    o.delivered_at,
    o.canceled_at,
    o.unavailable_at,
    o.estimated_delivery_at,
    o.units,
    o.commit_version as silver_version,
    o.change_type    as silver_change_type
from newest as o
left join {{ ref('int_customer_order_link') }} as l
    on l.customer_id = o.customer_id
{%- if not is_incremental() %}
where o.change_type <> 'delete'
{%- endif %}
