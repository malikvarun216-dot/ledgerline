{#- A snapshot may be fed only a night it does not hold yet, or the newest one again (Drill 3, B3).

    The snapshot compares the source with its OPEN rows: a key missing from the source is closed
    (hard_deletes='invalidate'). Fed an OLDER night than
    it holds, every key that night did not yet have is "missing" — Drill 3 fed night 4 to a history
    holding night 8: 73,298 living customers were closed at run time (closed rows 412 → 73,710), with
    rc 0, no error, and `snapshot_nights.py` (which only looks at MAX(silver_night)) would never repair it.
    The newest night again changes nothing (B4), so it stays allowed: a retry is safe. -#}
{% macro refuse_old_night(night) %}
  {%- if not execute -%}{{ return('') }}{%- endif -%}
  {%- set held = adapter.get_relation(this.database, this.schema, this.identifier) -%}
  {%- if held is none -%}{{ return('') }}{%- endif -%}
  {%- set top = run_query("select max(silver_night) from " ~ this).columns[0].values()[0] -%}
  {%- if top is not none and night | int < top | int -%}
    {{ exceptions.raise_compiler_error(
        this.identifier ~ " already holds night " ~ top ~ "; feeding night " ~ night
        ~ " would close every key that night did not have yet (Drill 3). Feed only newer nights:"
        ~ " scripts/snapshot_nights.py") }}
  {%- endif -%}
  {{ return('') }}
{% endmacro %}
