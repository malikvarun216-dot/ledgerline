{#- Does a late delivery cost us ratings? Delivered orders in cohorts — purchase month x customer
    state x how late — with their review scores (Session 22).

    SUMS AND COUNTS, NOT AVERAGES, are stored: a report that rolls months or states together must
    divide the summed score by the summed count. Averaging the stored avg_score across rows would
    weight a cohort of 3 orders like one of 3,000. avg_score is here only for reading one row. -#}

select
    purchase_month,
    customer_state,
    lateness_bucket,
    lateness_bucket_order,
    is_late,
    count(*)                                             as delivered_orders,
    count(review_score)                                  as reviewed_orders,
    sum(review_score)                                    as score_sum,
    count_if(review_score <= 2)                          as low_score_orders,
    count_if(review_score = 5)                           as five_star_orders,
    count_if(reviewed_before_delivery)                   as reviewed_before_delivery,
    round(sum(review_score) / nullif(count(review_score), 0), 4) as avg_score
from {{ ref('delivery_promise') }}
group by 1, 2, 3, 4, 5
