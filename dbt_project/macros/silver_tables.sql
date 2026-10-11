{#-
  The Silver tables (seven, eight with Session 15's customer_order_link) as they reach RAW.SILVER_CHANGES: each one's key and every field its
  Parquet rows carry, with the type Snowflake reads each field as (TYPEOF of the VARIANT value).

  ONE list, used three ways, so they cannot disagree:
    - stage_silver()                types each field into a staging column
    - tests/raw_fields_declared.sql the FLATTEN check: a field (or a type) RAW holds that is not
                                    listed here fails the build — a column Silver added is seen,
                                    never silently dropped (decisions.md, Session 13: VARIANT first)
    - tests/no_conflicting_changes  the key, for "two different rows at one (key, version)"

  The three change-feed fields every row carries (_change_type, _commit_version,
  _commit_timestamp) are listed once, in change_feed_fields().

  Read from RAW on 2026-10-11 (Session 14) with
    SELECT DISTINCT k.value, TYPEOF(GET(RECORD, k.value))
    FROM RAW.SILVER_CHANGES, LATERAL FLATTEN(OBJECT_KEYS(RECORD)) k
  A NULL value is ABSENT from a VARIANT object (Parquet nulls are not written as keys) — so a field
  missing from some rows is normal, and the check looks only for fields that should not be there.
-#}

{% macro change_feed_fields() -%}
    {{ return({"_change_type": "VARCHAR", "_commit_version": "INTEGER", "_commit_timestamp": "TIMESTAMP_NTZ"}) }}
{%- endmacro %}

{% macro silver_tables() -%}
{%- set dims_lineage = {
    "_first_seen_dump_date": "DATE",
    "_last_changed_dump_date": "DATE",
    "_source_file": "VARCHAR",
    "_merged_at": "TIMESTAMP_NTZ",
} -%}
{%- set kafka_lineage = {"_kafka_partition": "INTEGER", "_kafka_offset": "INTEGER", "_merged_at": "TIMESTAMP_NTZ"} -%}
{%- set tables = {
    "customer": {
        "key": ["customer_unique_id"],
        "fields": dict({
            "customer_unique_id": "VARCHAR",
            "customer_zip_code_prefix": "VARCHAR",
            "customer_city": "VARCHAR",
            "customer_state": "VARCHAR",
            "dim_updated_at": "TIMESTAMP_NTZ",
        }, **dims_lineage),
    },
    "product": {
        "key": ["product_id"],
        "fields": dict({
            "product_id": "VARCHAR",
            "product_category_name": "VARCHAR",
            "product_weight_g": "INTEGER",
            "product_length_cm": "INTEGER",
            "product_height_cm": "INTEGER",
            "product_width_cm": "INTEGER",
            "product_photos_qty": "INTEGER",
            "dim_updated_at": "TIMESTAMP_NTZ",
        }, **dims_lineage),
    },
    "seller": {
        "key": ["seller_id"],
        "fields": dict({
            "seller_id": "VARCHAR",
            "seller_zip_code_prefix": "VARCHAR",
            "seller_city": "VARCHAR",
            "seller_state": "VARCHAR",
            "dim_updated_at": "TIMESTAMP_NTZ",
        }, **dims_lineage),
    },
    "customer_order_link": {
        "key": ["customer_id"],
        "fields": dict({
            "customer_id": "VARCHAR",
            "customer_unique_id": "VARCHAR",
            "dim_updated_at": "TIMESTAMP_NTZ",
        }, **dims_lineage),
    },
    "orders": {
        "key": ["order_id"],
        "fields": {
            "order_id": "VARCHAR",
            "customer_id": "VARCHAR",
            "status": "VARCHAR",
            "source_status": "VARCHAR",
            "created_at": "TIMESTAMP_NTZ",
            "approved_at": "TIMESTAMP_NTZ",
            "shipped_at": "TIMESTAMP_NTZ",
            "delivered_at": "TIMESTAMP_NTZ",
            "canceled_at": "TIMESTAMP_NTZ",
            "unavailable_at": "TIMESTAMP_NTZ",
            "estimated_delivery_at": "TIMESTAMP_NTZ",
            "units": "INTEGER",
            "_merged_at": "TIMESTAMP_NTZ",
        },
    },
    "order_items": {
        "key": ["order_id", "order_item_id"],
        "fields": dict({
            "order_id": "VARCHAR",
            "order_item_id": "INTEGER",
            "product_id": "VARCHAR",
            "seller_id": "VARCHAR",
            "sku_key": "VARCHAR",
            "price": "DOUBLE",
            "freight_value": "DOUBLE",
            "_event_id": "VARCHAR",
        }, **kafka_lineage),
    },
    "inventory": {
        "key": ["sku_key"],
        "fields": {
            "sku_key": "VARCHAR",
            "product_id": "VARCHAR",
            "seller_id": "VARCHAR",
            "stock_qty": "INTEGER",
            "seq": "INTEGER",
            "_event_id": "VARCHAR",
            "_event_ts": "TIMESTAMP_NTZ",
            "_kafka_partition": "INTEGER",
            "_kafka_offset": "INTEGER",
            "_merged_at": "TIMESTAMP_NTZ",
        },
    },
    "inventory_events": {
        "key": ["event_id"],
        "fields": dict({
            "event_id": "VARCHAR",
            "sku_key": "VARCHAR",
            "product_id": "VARCHAR",
            "seller_id": "VARCHAR",
            "op": "VARCHAR",
            "seq": "INTEGER",
            "stock_qty": "INTEGER",
            "prev_stock_qty": "INTEGER",
            "event_ts": "TIMESTAMP_NTZ",
            "produced_at": "TIMESTAMP_NTZ",
        }, **kafka_lineage),
    },
} -%}
{{ return(tables) }}
{%- endmacro %}


{#- The SQL type each VARIANT type is cast to in staging. Money stays DOUBLE here, as Silver holds
    it; Gold rounds it to NUMBER(12,2) where it is summed. -#}
{% macro staging_type(variant_type) -%}
    {%- set types = {
        "VARCHAR": "STRING",
        "INTEGER": "NUMBER(38,0)",
        "DOUBLE": "FLOAT",
        "DATE": "DATE",
        "TIMESTAMP_NTZ": "TIMESTAMP_NTZ",
    } -%}
    {{ return(types[variant_type]) }}
{%- endmacro %}


{#-
  One staging view: every change row of one Silver table, every field typed.

  A cast that fails (a value that does not fit its type) fails the model — on purpose: RAW keeps the
  row whatever it holds, and staging is where a wrong type is found, by name (decisions.md S13).

  `night` numbers the table's change-feed versions 1, 2, 3, ... For the dimensions each night's
  MERGE is one version, so night n = what Silver held after the n-th dump — the replay unit for
  exp_05 and for rebuilding the SCD2 snapshots.
-#}
{% macro stage_silver(table_name) -%}
{%- set spec = silver_tables()[table_name] -%}
select
{%- for field, vtype in spec["fields"].items() %}
    record:"{{ field }}"::{{ staging_type(vtype) }} as {{ field }},
{%- endfor %}
    record:"_change_type"::string                as change_type,
    record:"_commit_version"::number(38,0)       as commit_version,
    record:"_commit_timestamp"::timestamp_ntz    as commit_ts,
    dense_rank() over (order by record:"_commit_version"::number(38,0)) as night,
    hash(record)                                 as change_hash,
    source_file,
    source_row,
    loaded_at
from {{ source('raw', 'silver_changes') }}
where table_name = '{{ table_name }}'
{%- endmacro %}
