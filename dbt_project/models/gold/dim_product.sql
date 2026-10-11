-- SCD2 product: one row per version, valid_from / valid_to in BUSINESS time (dim_updated_at), except a
-- deletion, which closes the last version at the snapshot's RUN time (exp_05 C — dbt has no business
-- time for a row that is absent). Point in time: valid_from <= T AND T < valid_to.
{{ config(materialized='view') }}
select
    product_id,
    product_category_name, product_weight_g, product_length_cm, product_height_cm, product_width_cm, product_photos_qty,
    dbt_valid_from                              as valid_from,
    dbt_valid_to                                as valid_to,
    dbt_valid_to = '9999-12-31'::timestamp_ntz  as is_current,
    silver_night
from {{ ref('product_history') }}
