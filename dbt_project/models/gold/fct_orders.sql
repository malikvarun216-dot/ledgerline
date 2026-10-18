{#- One row per order, as Silver holds it now (Session 15; decisions.md "Gold facts ...").

    INCREMENTAL: each run reads the change rows that ARRIVED in RAW after the newest arrival already
    applied (`raw_loaded_at`, a high-water mark taken from this table) minus one hour, keeps each
    order's newest change, and applies it with the merge_changes strategy (macros/merge_changes.sql):
    insert, update, and an in-band delete. Its `>=` version guard keeps a late OLDER change from
    overwriting a newer one, so re-reading the hour is harmless. The first build reads the whole log
    and drops deleted orders.

    WHY ARRIVAL, NOT VERSION (decisions.md, S17; exp_06): a file Snowpipe loads late holds versions
    already below a version mark and was skipped for good: 13,431 of 99,441 orders wrong in the
    replay. The hour of overlap covers two files committed slightly out of order. A re-read change
    the table already holds (same or older version, nothing to re-link) is dropped BEFORE the MERGE:
    without that, the first nightly run after the switch rewrote all 99,441 orders, because the
    whole log had arrived within one hour (S17).

    LATE-ARRIVING LINK: an order's person (customer_unique_id) comes from another source, the nightly
    customer_order_link dump, which can arrive after the order. Such an order is loaded with NULL
    and SELECTED AGAIN on the first run after its link exists — its order change has not moved, so
    the high-water mark alone would never look at it again. -#}
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
    where loaded_at > (select dateadd(hour, -1, coalesce(max(raw_loaded_at), '1970-01-01'::timestamp_ltz))
                       from {{ this }})
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
    o.change_type    as silver_change_type,
    o.loaded_at      as raw_loaded_at
from newest as o
left join {{ ref('int_customer_order_link') }} as l
    on l.customer_id = o.customer_id
{%- if is_incremental() %}
left join {{ this }} as f
    on f.order_id = o.order_id
-- only a change that would do something: a new order, a newer version, or a person now linked
where f.order_id is null
   or o.commit_version > f.silver_version
   or (f.customer_unique_id is null and l.customer_unique_id is not null)
{%- else %}
where o.change_type <> 'delete'
{%- endif %}
