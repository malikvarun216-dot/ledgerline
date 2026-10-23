{#- Which sellers hurt us? One row per seller per purchase month, scored by written rules — no model
    (Session 22).

    Volume counts every order holding the seller's items; delivery and reviews count DELIVERED orders.
    A review is about a whole order, so it is credited to a seller only when the order had ONE seller
    (1,278 of 98,666 orders in the raw file had more): crediting it to each would give two sellers the
    same complaint, and splitting it would invent a share. Those reviews are counted aside, never lost.

    THE RULES (one month, at least 10 delivered single-or-multi-seller orders, else 'too few orders').
    Thresholds from the spread of 2,304 seller-months with 10+ orders in the raw file: late rate
    median 3.6%, 75th percentile 9.5%, 90th 18%; average score 10th percentile 3.6, 25th 3.94.
      at risk  late_rate >= 20%  or avg_score < 3.5   (about the worst tenth on either)
      watch    late_rate >= 10%  or avg_score < 4.0   (about the worst quarter)
      good     otherwise
    avg_score with no reviewed single-seller order is NULL and decides nothing. -#}

{%- set min_orders = 10 %}

with seller_orders as (
    select
        i.seller_id,
        i.order_id,
        count(*)                      as items,
        sum(i.price)                  as revenue,
        sum(i.freight_value)          as freight
    from {{ ref('fct_order_items') }} as i
    group by 1, 2
),

joined as (
    select
        so.*,
        date_trunc(month, o.created_at)::date as purchase_month,
        o.status,
        p.order_id is not null        as delivered,
        p.is_late,
        p.seller_count,
        p.review_score
    from seller_orders as so
    join {{ ref('fct_orders') }} as o on o.order_id = so.order_id
    left join {{ ref('delivery_promise') }} as p on p.order_id = so.order_id
),

monthly as (
    select
        seller_id,
        purchase_month,
        count(*)                                                    as orders,
        sum(items)                                                  as items,
        sum(revenue)::number(14, 2)                                 as revenue,
        sum(freight)::number(14, 2)                                 as freight,
        count_if(status = 'canceled')                               as canceled_orders,
        count_if(delivered)                                         as delivered_orders,
        count_if(delivered and is_late)                             as late_orders,
        count_if(delivered and seller_count = 1 and review_score is not null) as reviewed_orders,
        sum(iff(delivered and seller_count = 1, review_score, null)) as score_sum,
        count_if(delivered and seller_count = 1 and review_score <= 2) as low_score_orders,
        count_if(delivered and seller_count > 1 and review_score is not null) as shared_reviews_not_credited
    from joined
    group by 1, 2
),

rated as (
    select
        *,
        round(late_orders / nullif(delivered_orders, 0), 4)  as late_rate,
        round(score_sum / nullif(reviewed_orders, 0), 4)      as avg_score
    from monthly
)

select
    *,
    case
        when delivered_orders < {{ min_orders }}            then 'too few orders'
        when late_rate >= 0.20 or avg_score < 3.5           then 'at risk'
        when late_rate >= 0.10 or avg_score < 4.0           then 'watch'
        else 'good'
    end as tier
from rated
