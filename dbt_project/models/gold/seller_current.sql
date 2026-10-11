-- Each seller as Silver holds them now: newest change per seller_id, deletes dropped.
{{ current_state('stg_seller', ['seller_id'], [
    'seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state',
    'dim_updated_at', '_first_seen_dump_date', '_last_changed_dump_date',
]) }}
