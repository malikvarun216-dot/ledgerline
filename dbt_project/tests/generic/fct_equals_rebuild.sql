{#- An incremental fact must hold exactly what a full rebuild from the change log would (Session 15):
    the same keys, each at the same Silver version. Rows come back from either side —
    'only in fact' (a delete that never reached it, a row stuck at an old version) or
    'only in rebuild' (a change the high-water mark skipped: exp_06's late file, S17).
    Versions only, not every value: a fact row at the right version holds that change row's values. -#}
{% test fct_equals_rebuild(model, staging, key) %}
{%- set cols = key | join(', ') %}
with newest as (
    select {{ cols }}, commit_version as silver_version, change_type
    from {{ ref(staging) }}
    qualify row_number() over (partition by {{ cols }} order by commit_version desc) = 1
),

rebuild as (
    select {{ cols }}, silver_version from newest where change_type <> 'delete'
),

fact as (
    select {{ cols }}, silver_version from {{ model }}
)

select 'only in fact' as side, * from (select * from fact minus select * from rebuild)
union all
select 'only in rebuild' as side, * from (select * from rebuild minus select * from fact)
{% endtest %}
