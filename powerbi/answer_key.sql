-- ledgerline — the numbers the Power BI boards must show, computed in Snowflake (Session 23).
--
--     python scripts/sf.py --as-bi --file powerbi/answer_key.sql
--     python scripts/sf.py --as-bi --role LEDGERLINE_ANALYST_SP --file powerbi/answer_key.sql
--
-- Runs as the report's own user, so masks and the row policy apply exactly as they do to the report.
-- Every average is a sum divided by a count, as the marts store them — never an average of averages.

-- 1. Promise accuracy (delivery_promise: one row per delivered order).
SELECT COUNT(*)                                              AS delivered_orders,
       COUNT_IF(is_late)                                     AS late_orders,
       ROUND(COUNT_IF(is_late) / COUNT(*), 4)                AS late_share,
       ROUND(AVG(promised_days), 2)                          AS avg_promised_days,
       ROUND(AVG(actual_days), 2)                            AS avg_actual_days,
       ROUND(AVG(IFF(is_late, days_late, NULL)), 2)          AS avg_days_late_when_late,
       COUNT(DISTINCT customer_state)                        AS states
FROM LEDGERLINE.GOLD.DELIVERY_PROMISE;

-- 2. Rating cohorts: on time vs late (late_vs_rating sums back to delivery_promise).
SELECT is_late,
       SUM(delivered_orders)                                 AS delivered_orders,
       SUM(reviewed_orders)                                  AS reviewed_orders,
       ROUND(SUM(score_sum) / SUM(reviewed_orders), 4)       AS avg_score,
       ROUND(SUM(low_score_orders) / SUM(reviewed_orders), 4) AS low_score_share,
       SUM(reviewed_before_delivery)                         AS rated_before_arrival
FROM LEDGERLINE.GOLD.LATE_VS_RATING
GROUP BY is_late ORDER BY is_late;

-- 3. Rating by lateness bucket.
SELECT lateness_bucket_order, lateness_bucket,
       SUM(reviewed_orders)                                  AS reviewed_orders,
       ROUND(SUM(score_sum) / SUM(reviewed_orders), 4)       AS avg_score,
       ROUND(SUM(low_score_orders) / SUM(reviewed_orders), 4) AS low_score_share
FROM LEDGERLINE.GOLD.LATE_VS_RATING
GROUP BY 1, 2 ORDER BY 1;

-- 4. The trap, shown: averaging the stored per-cohort avg_score instead of dividing sums.
SELECT is_late,
       ROUND(AVG(avg_score), 4)                              AS wrong_avg_of_avgs,
       ROUND(SUM(score_sum) / SUM(reviewed_orders), 4)       AS right_avg
FROM LEDGERLINE.GOLD.LATE_VS_RATING
GROUP BY is_late ORDER BY is_late;

-- 5. Seller scorecard: seller-months per tier.
SELECT tier, COUNT(*) AS seller_months, COUNT(DISTINCT seller_id) AS sellers
FROM LEDGERLINE.GOLD.SELLER_SCORECARD
GROUP BY tier ORDER BY tier;

-- 6. Complaint trends: reviews by topic, and the negative share of each.
SELECT topic,
       SUM(reviews)                                          AS reviews,
       SUM(IFF(sentiment = 'negative', reviews, 0))          AS negative_reviews,
       ROUND(SUM(score_sum) / SUM(reviews), 4)               AS avg_score
FROM LEDGERLINE.GOLD.COMPLAINT_TRENDS
GROUP BY topic ORDER BY reviews DESC;

-- 7. The mask, from the report's side: cities in customer_current.
SELECT COUNT(*) AS customers, COUNT_IF(customer_city = '***') AS cities_masked,
       COUNT(DISTINCT customer_state) AS states
FROM LEDGERLINE.GOLD.CUSTOMER_CURRENT;
