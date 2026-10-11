-- Stock per SKU as Silver holds it now: newest change per sku_key, delisted SKUs dropped.
-- The inventory source's shape is "only the current state matters" — Gold keeps no history of it.
{{ current_state('stg_inventory', ['sku_key'], [
    'sku_key', 'product_id', 'seller_id', 'stock_qty', 'seq', '_event_ts',
]) }}
