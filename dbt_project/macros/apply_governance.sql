{#- Who may see what, re-applied after every build of a Gold model (Session 18; decisions.md
    "Who may see what on Snowflake Gold").

    dbt re-creates `table` and `view` models every build, and Snowflake's CREATE OR REPLACE drops
    every tag and policy on the object (s18_governance A4/A5/D6). Masking itself does NOT depend on
    this hook: the GOLD schema carries PII_CLASS = 'restricted' (09_governance.sql), so a text column
    nobody tagged is masked. This hook only:
      1. tags each text column with the class `_gold.yml` declares (config.meta.pii_class) —
         'public' opens it to readers, 'restricted' closes it even if an old 'public' tag lingers
         on an incremental table;
      2. attaches the row access policy to the column named by the model's meta.row_policy, if the
         object does not already have it (an incremental table keeps it between runs; adding it
         twice is an error).
    tests/gold_governed.sql fails the build if either did not happen. Snowflake only. -#}

{% macro apply_governance() %}
  {%- if not execute or target.type != 'snowflake' -%}{{ return('') }}{%- endif -%}
  {%- set gov = var('governance_schema', 'LEDGERLINE.GOVERNANCE') -%}
  {%- set kind = 'view' if model.config.materialized == 'view' else 'table' -%}

  {%- for col in model.columns.values() -%}
    {%- set cls = (col.get('config') or {}).get('meta', {}).get('pii_class') or col.get('meta', {}).get('pii_class') -%}
    {%- if cls -%}
      {%- do run_query("alter " ~ kind ~ " " ~ this ~ " modify column " ~ col.name
                       ~ " set tag " ~ gov ~ ".pii_class = '" ~ cls ~ "'") -%}
    {%- endif -%}
  {%- endfor -%}

  {%- set row_col = model.config.get('meta', {}).get('row_policy') -%}
  {%- if row_col -%}
    {%- set held = run_query("select count(*) from table(" ~ this.database ~ ".information_schema.policy_references("
                             ~ "ref_entity_name => '" ~ this ~ "', ref_entity_domain => '" ~ kind ~ "'))"
                             ~ " where policy_kind = 'ROW_ACCESS_POLICY'").columns[0].values()[0] -%}
    {%- if held == 0 -%}
      {%- do run_query("alter " ~ kind ~ " " ~ this ~ " add row access policy " ~ gov ~ ".state_rows on ("
                       ~ row_col ~ ")") -%}
    {%- endif -%}
  {%- endif -%}
  {{ return('') }}
{% endmacro %}
