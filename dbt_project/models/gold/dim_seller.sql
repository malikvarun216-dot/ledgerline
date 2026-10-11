-- SCD2 seller: one row per version, valid_from / valid_to in BUSINESS time (dim_updated_at), except a
-- deletion, which closes the last version at the snapshot's RUN time (exp_05 C — dbt has no business
-- time for a row that is absent). Point in time: valid_from <= T AND T < valid_to.
{{ config(materialized='view') }}
select
    seller_id,
    seller_zip_code_prefix, seller_city, seller_state,
    dbt_valid_from                              as valid_from,
    dbt_valid_to                                as valid_to,
    dbt_valid_to = '9999-12-31'::timestamp_ntz  as is_current,
    silver_night
from {{ ref('seller_history') }}
