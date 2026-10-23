{#- One row per customer review, as Silver holds it now (Session 22; decisions.md "Reviews arrive as a
    fifth nightly dump ...").

    Keyed on review_id + order_id: neither is unique alone (789 review ids sit on more than one
    order, 547 orders have more than one review). Same incremental shape as fct_order_items: change
    rows that arrived after the arrival mark (minus one hour, S17), newest per review, minus re-read
    changes the table already holds, merge_changes (in-band deletes). A review changes once — when
    it is answered — so an update here is that answer arriving.

    message_hash joins the AI labels (review_labels): the SHA-256 of the message with leading and
    trailing white space removed, computed exactly as the Databricks labelling step does — the same
    text gives the same hash on both platforms, which test review_labels_join_back checks in data. A
    message that is only white space has no hash: there is nothing to label (incidents.md 2026-10-21). -#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge_changes',
    unique_key=['review_id', 'order_id'],
    tmp_relation_type='table',
    on_schema_change='fail',
) }}

with changes as (
    select * from {{ ref('stg_review') }}
    {%- if is_incremental() %}
    where loaded_at > (select dateadd(hour, -1, coalesce(max(raw_loaded_at), '1970-01-01'::timestamp_ltz))
                       from {{ this }})
    {%- endif %}
),

newest as (
    select *
    from changes
    qualify row_number() over (partition by review_id, order_id order by commit_version desc) = 1
),

trimmed as (
    select
        *,
        -- [[:space:]], not \s: Snowflake's \s leaves the vertical tab (char 11), Java's \s — what
        -- Spark's labelling step trims with — removes it. [[:space:]] is Java's six exactly
        -- (space, \t, \n, \x0B, \f, \r), so one text gives one hash on both platforms (S22).
        nullif(regexp_replace(review_comment_message, '^[[:space:]]+|[[:space:]]+$', ''), '') as message_text
    from newest
)

select
    o.review_id,
    o.order_id,
    o.review_score,
    o.review_comment_title,
    o.review_comment_message,
    o.message_text is not null                    as has_message,
    sha2(o.message_text, 256)                     as message_hash,
    o.review_creation_date                        as review_created_at,
    o.review_answer_timestamp                     as review_answered_at,
    o.commit_version as silver_version,
    o.change_type    as silver_change_type,
    o.loaded_at      as raw_loaded_at
from trimmed as o
{%- if is_incremental() %}
left join {{ this }} as f
    on f.review_id = o.review_id and f.order_id = o.order_id
-- only a change that would do something: a new review or a newer version
where f.review_id is null
   or o.commit_version > f.silver_version
{%- else %}
where o.change_type <> 'delete'
{%- endif %}
