-- Which person each order's customer_id belongs to, as Silver holds it now (Session 15). The facts
-- join this to reach customer_unique_id, the key of the customer history (decisions.md, Session 15).
{{ current_state('stg_customer_order_link', ['customer_id'], [
    'customer_id', 'customer_unique_id', 'dim_updated_at',
]) }}
