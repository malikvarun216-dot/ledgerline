{#- Every table dbt built is PERMANENT (Session 16; incidents.md 2026-10-12).

    dbt-snowflake builds TRANSIENT tables unless told otherwise: no Fail-safe after Time Travel.
    Gold and the SCD2 history were transient for two sessions and nothing showed it; a transient
    table queries exactly like a permanent one. `+transient: false` is set in dbt_project.yml; this
    test fails on any table that slips past it (a new folder without the config, a model overriding it).

    Returns a row per transient table — empty = pass.

    It reads information_schema, not a model, so dbt would run it whenever it liked — the first run
    (Session 16) ran it BEFORE the Gold tables were rebuilt and flagged 6 that the same build then
    made permanent. These lines make it wait for every Gold table: -#}
-- depends_on: {{ ref('fct_orders') }}
-- depends_on: {{ ref('fct_order_items') }}
-- depends_on: {{ ref('customer_current') }}
-- depends_on: {{ ref('product_current') }}
-- depends_on: {{ ref('seller_current') }}
-- depends_on: {{ ref('inventory_current') }}
select table_schema, table_name, table_type, is_transient
from {{ target.database }}.information_schema.tables
where table_type = 'BASE TABLE'
  and is_transient = 'YES'
  and table_schema in (
    {%- if target.name == 'ci' %} '{{ target.schema | upper }}'
    {%- else %} 'STAGING', 'SNAPSHOTS', 'GOLD'
    {%- endif %})
