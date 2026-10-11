-- Each product as Silver holds it now: newest change per product_id, deletes dropped.
{{ current_state('stg_product', ['product_id'], [
    'product_id', 'product_category_name', 'product_weight_g', 'product_length_cm',
    'product_height_cm', 'product_width_cm', 'product_photos_qty',
    'dim_updated_at', '_first_seen_dump_date', '_last_changed_dump_date',
]) }}
