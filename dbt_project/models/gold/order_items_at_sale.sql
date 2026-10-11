{#- Every order item with the customer, product and seller AS THEY WERE WHEN THE ORDER WAS PLACED
    (Session 15) — the point-in-time join of the facts to the SCD2 dimensions:
        valid_from <= ordered_at AND ordered_at < valid_to
    A view, resolved at query time: a fact never stores a version key that a later snapshot night
    could make stale (decisions.md, Session 15). Left joins: an item whose version is not found
    keeps its row, with its *_found flag false.

    The customer's FIRST version, when none covers the order time (customer_inferred = true): the
    dumps show a person first at their latest observation before a night, so an earlier order in
    the same window precedes their first version. Graded against Olist's per-order addresses
    (Session 15): the strict join answered 97,653 orders, 97,651 right; the first version answered
    the 1,634 it could not, 1,591 right — instead of NULL. -#}
{{ config(materialized='view') }}

with first_version as (
    select customer_unique_id, customer_city, customer_state
    from {{ ref('dim_customer') }}
    qualify row_number() over (partition by customer_unique_id order by valid_from) = 1
)

select
    i.order_id,
    i.order_item_id,
    o.created_at                    as ordered_at,
    o.status                        as order_status,
    o.customer_unique_id,
    coalesce(c.customer_city, f.customer_city)   as customer_city,
    coalesce(c.customer_state, f.customer_state) as customer_state,
    i.product_id,
    p.product_category_name,
    i.seller_id,
    s.seller_city,
    s.seller_state,
    i.price,
    i.freight_value,
    c.customer_unique_id is not null as customer_found,
    c.customer_unique_id is null and f.customer_unique_id is not null as customer_inferred,
    p.product_id is not null         as product_found,
    s.seller_id is not null          as seller_found
from {{ ref('fct_order_items') }} as i
join {{ ref('fct_orders') }} as o
    on o.order_id = i.order_id
left join {{ ref('dim_customer') }} as c
    on  c.customer_unique_id = o.customer_unique_id
    and c.valid_from <= o.created_at and o.created_at < c.valid_to
left join first_version as f
    on f.customer_unique_id = o.customer_unique_id
left join {{ ref('dim_product') }} as p
    on  p.product_id = i.product_id
    and p.valid_from <= o.created_at and o.created_at < p.valid_to
left join {{ ref('dim_seller') }} as s
    on  s.seller_id = i.seller_id
    and s.valid_from <= o.created_at and o.created_at < s.valid_to
