{#- SCD2 history of Silver's `customer` (Session 14; decisions.md "Gold dimension history is a dbt snapshot").
    timestamp strategy on dim_updated_at = BUSINESS time (exp_05: run time answers no 2017 question);
    Deletes (Drill 3): no hard_deletes. A deleted key arrives from dims_state as an is_deleted row dated
    by its dump's business date, so its last version closes THERE, not at the run time (exp_05 C's
    run-time date made the history unrebuildable: 1,050 rows).
    Run only through scripts/snapshot_nights.py: one Silver night per run, oldest first. A single run
    over two new nights would keep only the newer one. -#}
{% snapshot customer_history %}
{{ config(
    unique_key='customer_unique_id',
    strategy='timestamp',
    updated_at='dim_updated_at',
    dbt_valid_to_current="to_timestamp_ntz('9999-12-31')"
) }}
{%- if execute and var('as_of_night') is none -%}
    {{ exceptions.raise_compiler_error(
        this.identifier ~ " runs one Silver night at a time: use scripts/snapshot_nights.py "
        ~ "(a run over several new nights keeps only the newest; decisions.md, Session 14)") }}
{%- endif %}
{{ refuse_old_night(var('as_of_night')) }}
{{ dims_state('stg_customer', ['customer_unique_id'], [
    'customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state',
    'dim_updated_at',
], var('as_of_night')) }}
{% endsnapshot %}
