-- SCD2 customer: one row per version, valid_from / valid_to in BUSINESS time (dim_updated_at). A deletion
-- closes the last version at the business date of the dump that dropped it (Drill 3); the snapshot's
-- is_deleted row after it is left out here, so a deleted key has no version from that date on.
-- Point in time: valid_from <= T AND T < valid_to.
{{ config(materialized='view') }}
select
    customer_unique_id,
    customer_zip_code_prefix, customer_city, customer_state,
    dbt_valid_from                              as valid_from,
    dbt_valid_to                                as valid_to,
    dbt_valid_to = '9999-12-31'::timestamp_ntz  as is_current,
    silver_night
from {{ ref('customer_history') }}
where is_deleted is distinct from true   -- the deleted row marks the end; it is not a version
