{#-
  SCD2 shape: at most one open version per key, and each version ends exactly where the key's next
  one starts (no gap, no overlap). Returns the keys that break either rule.
-#}
{% test scd2_shape(model, key) %}
with versions as (
    select {{ key }}, valid_from, valid_to,
           lead(valid_from) over (partition by {{ key }} order by valid_from) as next_from
    from {{ model }}
)
select {{ key }}, 'two open versions' as problem from versions
where valid_to = '9999-12-31'::timestamp_ntz
group by {{ key }} having count(*) > 1
union all
select {{ key }}, 'gap or overlap' from versions
where next_from is not null and next_from <> valid_to
{% endtest %}
