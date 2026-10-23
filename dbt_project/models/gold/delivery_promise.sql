{#- Are our delivery promises honest? One row per delivered order: what was promised, when it came,
    and what the customer said (Session 22).

    Promise = Olist's estimated delivery DATE shown at purchase; late = delivered on a later calendar
    day. Days are counted on calendar dates, because the promise is a date with no time.

    customer_state = where the person lived WHEN THEY ORDERED, from the point-in-time view (the SCD2
    customer as of the order time), not where they live now. It is also the row policy's column.

    The review: an order can have more than one (547 do); the newest by creation date stands for the
    order, ties broken by answer time, then review_id, so the choice is deterministic.
    reviewed_before_delivery: the survey goes out around the promised date, so a customer whose parcel
    is late may rate it before it arrives — 4,967 orders in the raw file. -#}

with delivered as (
    select order_id, customer_unique_id, status, created_at, estimated_delivery_at, delivered_at
    from {{ ref('fct_orders') }}
    where delivered_at is not null
      and estimated_delivery_at is not null
),

at_sale as (
    select
        order_id,
        any_value(customer_state)  as customer_state,
        count(distinct seller_id)  as seller_count,
        count(*)                   as items
    from {{ ref('order_items_at_sale') }}
    group by order_id
),

newest_review as (
    select order_id, review_score, review_created_at
    from {{ ref('fct_reviews') }}
    qualify row_number() over (
        partition by order_id
        order by review_created_at desc, review_answered_at desc nulls last, review_id
    ) = 1
),

days as (
    select
        d.*,
        datediff(day, d.created_at::date, d.estimated_delivery_at::date) as promised_days,
        datediff(day, d.created_at::date, d.delivered_at::date)          as actual_days,
        -- negative = early; 0 = on the promised day
        datediff(day, d.estimated_delivery_at::date, d.delivered_at::date) as days_late
    from delivered as d
)

select
    d.order_id,
    d.customer_unique_id,
    s.customer_state,
    s.seller_count,
    s.items,
    d.status                         as order_status,
    d.created_at                     as purchased_at,
    date_trunc(month, d.created_at)::date as purchase_month,
    d.estimated_delivery_at          as promised_at,
    d.delivered_at,
    d.promised_days,
    d.actual_days,
    d.days_late,
    d.days_late > 0                  as is_late,
    {{ lateness_bucket('d.days_late') }} as lateness_bucket,
    {{ lateness_bucket_order('d.days_late') }} as lateness_bucket_order,
    r.review_score,
    r.review_created_at              as reviewed_at,
    r.review_created_at::date < d.delivered_at::date as reviewed_before_delivery
from days as d
left join at_sale as s on s.order_id = d.order_id
left join newest_review as r on r.order_id = d.order_id
