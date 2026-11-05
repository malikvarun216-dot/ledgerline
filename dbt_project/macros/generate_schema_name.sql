{#-
  Use the schema each folder names, exactly — STAGING, SNAPSHOTS, GOLD, S14.

  dbt's default is "<profile schema>_<custom schema>" (STAGING_GOLD), made so every developer can
  build into a schema of their own. Here the transformer cannot create schemas at all: SYSADMIN
  made these three once (snowflake/06_gold_schemas.sql; decisions.md, Session 14), so a prefixed
  name would be a schema that does not exist and the run would fail on its first model.

  Target `ci` (Session 16): EVERY node goes into the run's own schema (target.schema, named by the
  slim CI script), in database LEDGERLINE_CI. One throwaway schema per run, dropped after; model
  names are unique across the project, so one schema holds staging and Gold side by side.

  `--vars "{wap: true}"` (Session 25, write-audit-publish; scripts/publish_gold.py): Gold is built in
  its audit schema instead, GOLD_AUDIT (made by SYSADMIN, 17_gold_audit.sql), or <run schema>_AUDIT
  on `ci`. Everything else is still read where it lives, so the audit build reads the published
  staging views and snapshots.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set audit = var('wap', false) and (custom_schema_name or '') | trim | lower == 'gold' -%}
    {%- if target.name == 'ci' or custom_schema_name is none -%}
        {{ target.schema }}{{ '_AUDIT' if audit else '' }}
    {%- else -%}
        {{ 'gold_audit' if audit else custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
