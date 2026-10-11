-- The cross-source tie-out, rebuilt on Gold (Session 15). Per SKU: units SOLD (fct_order_items, one
-- row = one unit) against units the stock feed DECREMENTED (inventory events: prev_stock_qty -
-- stock_qty where stock fell). Two sources, two Kafka topics, one answer — the definition Silver
-- uses (databricks/dashboards/pipeline_health/reconciliation.sql): 34,448 SKUs, 112,650 = 112,650.
-- Per SKU, not the total: two SKUs off by opposite amounts cancel in a sum. FULL OUTER JOIN: a SKU
-- on one side only disagrees too. Returns the SKUs that disagree; none = pass.
with sold as (
    select sku_key, count(*) as units
    from {{ ref('fct_order_items') }}
    group by sku_key
),

events as (
    -- newest change per event, then drop deletes (B68's order); a file loaded twice counts once
    select *
    from {{ ref('stg_inventory_events') }}
    qualify row_number() over (partition by event_id order by commit_version desc) = 1
),

decremented as (
    select sku_key, sum(prev_stock_qty - stock_qty) as units
    from events
    where change_type <> 'delete' and prev_stock_qty > stock_qty
    group by sku_key
)

select
    coalesce(s.sku_key, d.sku_key) as sku_key,
    coalesce(s.units, 0)           as units_sold,
    coalesce(d.units, 0)           as units_decremented
from sold as s
full outer join decremented as d
    on d.sku_key = s.sku_key
where coalesce(s.units, 0) <> coalesce(d.units, 0)
