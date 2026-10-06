-- Tables per schema against Unity Catalog's quota of 100 per schema (incidents.md, 2026-10-06:
-- experiment scratch filled 73 of workspace.silver's; Databricks emails at 80%).
SELECT table_schema, count(*) AS tables, 100 - count(*) AS headroom
FROM workspace.information_schema.tables
WHERE table_schema <> 'information_schema'
GROUP BY table_schema
ORDER BY tables DESC
