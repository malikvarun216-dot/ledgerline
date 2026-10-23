{#- The three marts of Session 22 add up to their sources. One row per disagreement — empty = pass.

    1. delivery_promise holds every order of fct_orders with a delivery and a promise, once.
    2. late_vs_rating's cohorts sum back to delivery_promise: orders, reviews, scores.
    3. seller_scorecard has one row per (seller, month), and its orders sum back to the distinct
       (seller, order) pairs of fct_order_items — no order lost or counted twice by the joins.
    4. fct_reviews: one row per (review_id, order_id). -#}

with expected_orders as (
    select count(*) as n from {{ ref('fct_orders') }}
    where delivered_at is not null and estimated_delivery_at is not null
),

promise as (
    select
        count(*) as n,
        count(distinct order_id) as distinct_n,
        count(review_score) as reviewed,
        sum(review_score) as score_sum
    from {{ ref('delivery_promise') }}
),

cohorts as (
    select sum(delivered_orders) as n, sum(reviewed_orders) as reviewed, sum(score_sum) as score_sum
    from {{ ref('late_vs_rating') }}
),

scorecard as (
    select count(*) as n, count(distinct seller_id, purchase_month) as distinct_n, sum(orders) as orders
    from {{ ref('seller_scorecard') }}
),

seller_orders as (
    select count(distinct seller_id, order_id) as n from {{ ref('fct_order_items') }}
),

reviews as (
    select count(*) as n, count(distinct review_id, order_id) as distinct_n from {{ ref('fct_reviews') }}
)

select 'delivery_promise rows vs fct_orders' as problem, p.n as got, e.n as expected
from promise as p, expected_orders as e where p.n <> e.n
union all
select 'delivery_promise duplicate orders', p.n, p.distinct_n from promise as p where p.n <> p.distinct_n
union all
select 'late_vs_rating orders', c.n, p.n from cohorts as c, promise as p where c.n <> p.n
union all
select 'late_vs_rating reviews', c.reviewed, p.reviewed from cohorts as c, promise as p where c.reviewed <> p.reviewed
union all
select 'late_vs_rating score sum', c.score_sum, p.score_sum from cohorts as c, promise as p where c.score_sum <> p.score_sum
union all
select 'seller_scorecard duplicate seller-months', s.n, s.distinct_n from scorecard as s where s.n <> s.distinct_n
union all
select 'seller_scorecard orders vs order items', s.orders, o.n from scorecard as s, seller_orders as o where s.orders <> o.n
union all
select 'fct_reviews duplicate keys', r.n, r.distinct_n from reviews as r where r.n <> r.distinct_n
