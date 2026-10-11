{#-
  Drop the slim CI run's schema (Session 16). Called by scripts/slim_ci.py when the run ends,
  pass or fail, so LEDGERLINE_CI does not fill up with throwaway builds.

  Refuses unless the target is `ci` AND the database is LEDGERLINE_CI: run against the production
  target by mistake, this would otherwise drop STAGING (the profile's default schema). The CI role
  could not drop it anyway — it owns nothing in LEDGERLINE — but the guard does not rely on that.
-#}
{% macro drop_ci_schema() %}
    {%- if target.name != 'ci' or target.database | upper != 'LEDGERLINE_CI' -%}
        {{ exceptions.raise_compiler_error(
            "drop_ci_schema refused: target " ~ target.name ~ " / database " ~ target.database
            ~ " — only the ci target in LEDGERLINE_CI may be dropped") }}
    {%- endif -%}
    {%- set relation = target.database ~ "." ~ target.schema -%}
    {% do run_query("drop schema if exists " ~ relation) %}
    {{ log("dropped schema " ~ relation, info=True) }}
{% endmacro %}
