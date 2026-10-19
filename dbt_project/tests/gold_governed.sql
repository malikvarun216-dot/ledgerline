{#- Every Gold object carries the governance `_gold.yml` declares for it (Session 18; decisions.md
    "Who may see what on Snowflake Gold").

    dbt's CREATE OR REPLACE drops tags and policies silently (s18_governance A4/A5/D6), and a
    missing one looks like nothing at all: no error, the reader just sees more. The post-hook
    `apply_governance` puts them back; this test proves it did. One row per problem — empty = pass:
      - a column whose PII_CLASS tag differs from its declared config.meta.pii_class
      - an object declaring meta.row_policy with no row access policy on it
      - (production only) the GOLD schema without PII_CLASS = 'restricted', the fail-closed default

    It reads tags and policies, not a model, so these lines make it wait for every Gold object: -#}
-- depends_on: {{ ref('customer_current') }}
-- depends_on: {{ ref('product_current') }}
-- depends_on: {{ ref('seller_current') }}
-- depends_on: {{ ref('inventory_current') }}
-- depends_on: {{ ref('dim_customer') }}
-- depends_on: {{ ref('dim_product') }}
-- depends_on: {{ ref('dim_seller') }}
-- depends_on: {{ ref('fct_orders') }}
-- depends_on: {{ ref('fct_order_items') }}
-- depends_on: {{ ref('order_items_at_sale') }}
{%- set gov = var('governance_schema', 'LEDGERLINE.GOVERNANCE') %}
{%- set checks = [] %}
{%- if execute and target.type == 'snowflake' %}  {#- tags and policies are Snowflake's; the databricks target has neither #}
  {%- for node in graph.nodes.values()
        if node.resource_type == 'model' and node.package_name == 'ledgerline' and node.fqn[1] == 'gold' %}
    {%- set rel = node.relation_name %}
    {%- set kind = 'view' if node.config.materialized == 'view' else 'table' %}
    {%- for col in node.columns.values() %}
      {%- set cls = (col.get('config') or {}).get('meta', {}).get('pii_class') or col.get('meta', {}).get('pii_class') %}
      {%- if cls %}
        {%- do checks.append(
            "select '" ~ rel ~ "." ~ col.name ~ "' as object, 'column tag ' || coalesce(v, 'none') || ', declared "
            ~ cls ~ "' as problem from (select system$get_tag('" ~ gov ~ ".pii_class', '" ~ rel ~ "." ~ col.name
            ~ "', 'column') as v) where coalesce(v, '') <> '" ~ cls ~ "'") %}
      {%- endif %}
    {%- endfor %}
    {%- if node.config.get('meta', {}).get('row_policy') %}
      {%- do checks.append(
          "select '" ~ rel ~ "' as object, 'no row access policy' as problem where not exists (select 1 from table("
          ~ node.database ~ ".information_schema.policy_references(ref_entity_name => '" ~ rel
          ~ "', ref_entity_domain => '" ~ kind ~ "')) where policy_kind = 'ROW_ACCESS_POLICY')") %}
    {%- endif %}
  {%- endfor %}
  {%- if target.name == 'snowflake' %}
    {%- do checks.append(
        "select '" ~ target.database ~ ".GOLD' as object, 'schema tag ' || coalesce(v, 'none') || ', expected restricted'"
        ~ " as problem from (select system$get_tag('" ~ gov ~ ".pii_class', '" ~ target.database
        ~ ".GOLD', 'schema') as v) where coalesce(v, '') <> 'restricted'") %}
  {%- endif %}
{%- endif %}
{%- if checks %}
{{ checks | join('\nunion all\n') }}
{%- else %}
select null as object, null as problem where false
{%- endif %}
