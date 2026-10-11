-- Each person as Silver holds them now: newest change per customer_unique_id, deletes dropped.
-- Rebuilt whole on every run (decisions.md, Session 14 "Gold current state is rebuilt").
{{ current_state('stg_customer', ['customer_unique_id'], [
    'customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state',
    'dim_updated_at', '_first_seen_dump_date', '_last_changed_dump_date',
]) }}
