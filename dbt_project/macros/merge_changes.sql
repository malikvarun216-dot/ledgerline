{#-
  A custom incremental strategy (Session 15): apply a batch of Silver CHANGES to a fact table —
  inserts, updates AND deletes — in one MERGE. Used as `incremental_strategy='merge_changes'`.

  Why not dbt's own `merge` (decisions.md, Session 15): it writes only UPDATE and INSERT. A change
  row whose `_change_type` is 'delete' would be UPDATEd onto the fact (the order stays, now saying
  "delete" in a column) or, for a key the fact never had, INSERTed. Deletes arrive IN-BAND in the
  change log (CLAUDE.md rule 1), so the MERGE must read the flag, the way Silver's inventory MERGE
  does — never infer a delete from a key being absent from the batch.

  The model's SELECT must return, per unique key, at most one row (its newest change) with:
    silver_version      the change-feed version the row comes from
    silver_change_type  'insert' / 'update_postimage' / 'delete'

    match + delete  + not older  -> DELETE
    match + other   + not older  -> UPDATE every column
    no match + delete            -> nothing (a delete for a row never loaded)
    no match + other             -> INSERT

  "Not older" (`>=`, not `>`): the same version is re-applied when a row is re-tried (a fact whose
  dimension link was missing is selected again with its unchanged change row), and a late-loaded
  OLDER change can never overwrite a newer one already in the fact.

  dbt calls this macro with arg_dict = {target_relation, temp_relation, unique_key, dest_columns,
  incremental_predicates}; the first build of the model (and --full-refresh) never calls it.
-#}
{% macro get_incremental_merge_changes_sql(arg_dict) -%}
    {%- set target = arg_dict["target_relation"] -%}
    {%- set source = arg_dict["temp_relation"] -%}
    {%- set keys = arg_dict["unique_key"] -%}
    {%- if keys is string -%}{%- set keys = [keys] -%}{%- endif -%}
    {%- set columns = arg_dict["dest_columns"] | map(attribute="name") | list -%}
    {%- set lower = columns | map("lower") | list -%}
    {%- if "silver_version" not in lower or "silver_change_type" not in lower -%}
        {{ exceptions.raise_compiler_error(
            target.identifier ~ ": merge_changes needs silver_version and silver_change_type columns") }}
    {%- endif %}
merge into {{ target }} as t
using {{ source }} as s
on {% for key in keys %}t.{{ key }} = s.{{ key }}{% if not loop.last %} and {% endif %}{% endfor %}
when matched and s.silver_change_type = 'delete' and s.silver_version >= t.silver_version then delete
when matched and s.silver_change_type <> 'delete' and s.silver_version >= t.silver_version then update set
    {%- for column in columns %}
    t.{{ column }} = s.{{ column }}{% if not loop.last %},{% endif %}
    {%- endfor %}
when not matched and s.silver_change_type <> 'delete' then insert
    ({{ columns | join(', ') }})
    values ({% for column in columns %}s.{{ column }}{% if not loop.last %}, {% endif %}{% endfor %})
{%- endmacro %}
