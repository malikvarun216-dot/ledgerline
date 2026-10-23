{#- No review text in Snowflake looks like contact data (Session 22).

    no_contact_data_in_snowflake checks column NAMES; review text is free, so a customer could type an
    email, a phone number or an ID number into it, and a name check would never see it. Snowflake holds
    nothing an erasure must delete only while that stays false (decisions.md, Session 18).

    First build (S22): 7 review TITLES held 8-14 digit runs (order numbers, and numbers that may be a
    phone or a CPF). Silver now redacts them (free_text, databricks/silver/_merge_dims.py REDACT) and
    the older RAW rows were rewritten (snowflake/repairs/2026-10-24_redact_review_text.sql). Checked in
    Gold AND in RAW, where every past version of a review lives. One row per match — empty = pass. -#}
{%- set pattern = '.*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}|[0-9]{4,5}-?[0-9]{4}).*' %}

select 'gold' as layer, review_id, order_id
from {{ ref('fct_reviews') }}
where regexp_like(coalesce(review_comment_message, '') || ' ' || coalesce(review_comment_title, ''),
                  '{{ pattern }}', 's')

union all

select 'raw', record:review_id::string, record:order_id::string
from {{ source('raw', 'silver_changes') }}
where table_name = 'review'
  and regexp_like(coalesce(record:review_comment_message::string, '') || ' '
                  || coalesce(record:review_comment_title::string, ''), '{{ pattern }}', 's')
