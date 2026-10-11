{#- exp_05 (Session 14), scratch schema S14, customer only — the dimension whose history is real.
    BUSINESS time, but run ONCE on today's state, no replay: a snapshot started late.
    Built only by snowflake/experiments/exp_05_scd2.py (--vars "{exp05: true, as_of_night: n}"). -#}
{% snapshot exp05_ts_one_run %}
{{ config(
    enabled=var('exp05', false),
    schema='s14',
    unique_key='customer_unique_id',
    strategy='timestamp', updated_at='dim_updated_at', hard_deletes='invalidate',
    dbt_valid_to_current="to_timestamp_ntz('9999-12-31')"
) }}
{{ current_state('stg_customer', ['customer_unique_id'], [
    'customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state', 'dim_updated_at',
], as_of_night=var('as_of_night')) }}
{% endsnapshot %}
