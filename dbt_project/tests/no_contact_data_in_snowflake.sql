{#- No contact data ever reaches Snowflake (Session 18; decisions.md "Erasure on Snowflake").

    Erasure (S12) deletes a person's CONTACT data — name, email, phone, email hash — on Databricks.
    The pseudonymous customer_unique_id, city, state and orders stay. Snowflake therefore holds
    nothing an erasure must delete ONLY as long as contact data never crosses the bridge. This test
    keeps that true. One row per contact-looking name — empty = pass:
      1. a field declared for the bridge in silver_tables() (RAW can hold no other field:
         raw_fields_declared fails the build on any undeclared one);
      2. a column anywhere in this database (a model, a seed, a table someone made by hand).

    The pattern was checked on 12 names (S18): it flags email_hash, phone, customer_name,
    street_address and passes the real fields, product_category_name included. Snowflake only. -#}
-- depends_on: {{ ref('order_items_at_sale') }}
{%- set contact = ".*(^|_)(email|e_mail|phone|mobile|cpf|cnpj|ssn|passport|birth|dob|address|street)(_|$).*"
                  ~ "|.*(^|_)(first|last|full|given|family|contact|customer|person)_name(_|$).*" %}
{%- if target.type == 'snowflake' %}
with declared as (
{%- for table_name, spec in silver_tables().items() %}
  {%- for field in spec.fields %}
    select 'bridge field' as kind, '{{ table_name }}' as object, '{{ field }}' as name union all
  {%- endfor %}
{%- endfor %}
    select 'bridge field', null, null where false
),

columns_ as (
    select 'column' as kind, table_schema || '.' || table_name as object, column_name as name
    from {{ target.database }}.information_schema.columns
)

select kind, object, name
from (select * from declared union all select * from columns_)
where regexp_like(lower(name), '{{ contact }}')
{%- else %}
select null as kind, null as object, null as name where false
{%- endif %}
