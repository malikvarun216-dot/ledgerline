{#- Every stored ratio equals round(numerator / denominator, 4) recomputed from its own row
    (incidents.md 2026-11-06). Snowflake stored these as NUMBER(38,0) from S22 to S28 — the contract
    said `number` — so 0.0625 read 0 and 4.0968 read 4, and no test looked at a ratio's value.
    Tolerance 0.00005: half the last kept place, so a DOUBLE (Databricks) passes and a lost place fails.
    Expected values come from ratio() (one rounding, 12 places first): Snowflake's own a / b keeps 6
    places and rounded 902 / 203 to 4.4434 — a test recomputing it that way agreed with the bug.
    One row per wrong value — empty = pass. -#}
select 'seller_scorecard' as model, 'late_rate' as col, seller_id as key_, purchase_month,
       late_rate as stored, {{ ratio('late_orders', 'delivered_orders') }} as expected
from {{ ref('seller_scorecard') }}
where abs(coalesce(late_rate, -1) - coalesce({{ ratio('late_orders', 'delivered_orders') }}, -1)) > 0.00005
union all
select 'seller_scorecard', 'avg_score', seller_id, purchase_month,
       avg_score, {{ ratio('score_sum', 'reviewed_orders') }}
from {{ ref('seller_scorecard') }}
where abs(coalesce(avg_score, -1) - coalesce({{ ratio('score_sum', 'reviewed_orders') }}, -1)) > 0.00005
union all
select 'late_vs_rating', 'avg_score', customer_state || ' / ' || lateness_bucket, purchase_month,
       avg_score, {{ ratio('score_sum', 'reviewed_orders') }}
from {{ ref('late_vs_rating') }}
where abs(coalesce(avg_score, -1) - coalesce({{ ratio('score_sum', 'reviewed_orders') }}, -1)) > 0.00005
