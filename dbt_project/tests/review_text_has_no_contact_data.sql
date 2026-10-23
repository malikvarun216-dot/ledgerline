{#- No review text that reaches Snowflake looks like contact data (Session 22).

    no_contact_data_in_snowflake checks column NAMES; review text is free, so a customer could type an
    email or a phone number into it, and a name check would never see it. Snowflake holds nothing an
    erasure must delete only while that stays false (decisions.md, Session 18). Measured on the raw
    file before the lane was built: 0 of 40,977 messages matched either pattern. One row per match —
    empty = pass; a match means the text must be scrubbed on Databricks before the bridge. -#}

select review_id, order_id, 'looks like contact data' as problem
from {{ ref('fct_reviews') }}
where regexp_like(coalesce(review_comment_message, '') || ' ' || coalesce(review_comment_title, ''),
                  '.*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}|[0-9]{4,5}-?[0-9]{4}).*', 's')
