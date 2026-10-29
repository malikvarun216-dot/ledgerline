{#-
  A dimension's state as of one night, for the SCD2 snapshots: like current_state, but a DELETED key
  stays in, as a row flagged is_deleted and dated by the business date of the dump that no longer had
  it (decisions.md, "A deleted dimension row ends on the business date of the dump", Drill 3).

  Why not hard_deletes: dbt finds a hard delete by absence (an anti-join) and stamps it with the run's
  clock — the live history said 2026-10-11 19:45 for a customer the 2018-12-23 dump dropped, and a
  rebuild said 2026-10-29 (Drill 3, 1,050 rows). Here the delete is an ordinary change with a newer
  dim_updated_at, so the timestamp strategy closes the old version AT the dump date and opens a deleted
  version. Every date now comes from the data, so a rebuild reproduces the history exactly.

  The night's dump date = the newest `_last_changed_dump_date` among that night's non-delete changes
  (every night has some; a delete row carries its preimage, so its own dump date is the old one).
  Midnight of that date is after every business update the dump held (checked: 1,050 of 1,050).
-#}
{% macro dims_state(staging_model, key, columns, as_of_night) -%}
with changes as (
    select * from {{ ref(staging_model) }}
    where night <= {{ as_of_night | int }}
),

night_dump as (
    select night, max(_last_changed_dump_date)::timestamp_ntz as dump_at
    from changes
    where change_type <> 'delete'
    group by night
),

newest as (
    select *
    from changes
    qualify row_number() over (
        partition by {{ key | join(', ') }}
        order by commit_version desc
    ) = 1
)

select
    {%- for c in columns %}
    {%- if c == 'dim_updated_at' %}
    case when n.change_type = 'delete' then d.dump_at else n.dim_updated_at end as dim_updated_at,
    {%- else %}
    n.{{ c }},
    {%- endif %}
    {%- endfor %}
    n.change_type = 'delete' as is_deleted,
    n.commit_version         as silver_version,
    n.night                  as silver_night
from newest as n
left join night_dump as d
    on d.night = n.night
{%- endmacro %}
