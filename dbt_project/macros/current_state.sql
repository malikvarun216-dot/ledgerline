{#-
  The current state of one Silver table, rebuilt from its change log in staging.

  For each key: take its newest change (highest commit_version), THEN drop the key if that change is
  a delete. The order matters (B68): filtering deletes out first would leave the key's older insert
  as its "newest" change, and a deleted row would come back.

  as_of_night: only changes up to that night (the n-th change-feed version of the table) — Silver as
  it stood after the n-th dims dump. Used by the snapshot replay (exp_05 and rebuilds); unset = now.

  A key's change rows can be duplicated (a file loaded twice) — they are identical, so any one of
  them is the same answer; two DIFFERENT rows at one (key, version) are refused before this, by the
  no_conflicting_changes test on staging.
-#}
{% macro current_state(staging_model, key, columns, as_of_night=none) -%}
with changes as (
    select * from {{ ref(staging_model) }}
    {%- if as_of_night is not none %}
    where night <= {{ as_of_night | int }}
    {%- endif %}
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
    {{ columns | join(',\n    ') }},
    commit_version as silver_version,
    night          as silver_night
from newest
where change_type <> 'delete'
{%- endmacro %}
