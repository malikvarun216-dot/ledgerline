{#-
  The schema-drift check: every field RAW holds, with its type, must be declared in
  macros/silver_tables.sql. Returns rows only when something is undeclared — a pass is empty.

  Why: staging types the fields it knows by name. A field Silver adds later would sit in RECORD,
  unread, and every model would stay green — the silent drop of Delta's `INSERT *` (S11 E3), one
  layer later. FLATTEN(OBJECT_KEYS(RECORD)) turns each row's object into one row per key, so the
  fields present can be listed and compared with the declaration.

  It catches: a new field; a field whose type changed (an INTEGER arriving as DOUBLE); a
  _change_type value other than the three the export writes.
  It cannot catch: a field that disappeared — a NULL value is absent from a VARIANT object anyway,
  so "missing in some rows" is normal (see silver_tables.sql).
-#}
{%- set feed = change_feed_fields() -%}
with declared as (
{%- for table_name, spec in silver_tables().items() %}
{%- for field, vtype in spec["fields"].items() %}
    select '{{ table_name }}' as table_name, '{{ field }}' as field, '{{ vtype }}' as variant_type union all
{%- endfor %}
{%- for field, vtype in feed.items() %}
    select '{{ table_name }}', '{{ field }}', '{{ vtype }}'{{ " union all" if not loop.last }}
{%- endfor %}
{{- " union all" if not loop.last }}
{%- endfor %}
),

seen as (
    select r.table_name,
           k.value::string                          as field,
           typeof(get(r.record, k.value::string))   as variant_type,
           count(*)                                 as rows_
    from {{ source('raw', 'silver_changes') }} r,
         lateral flatten(input => object_keys(r.record)) k
    group by 1, 2, 3
)

select 'field or type not declared' as problem, s.table_name, s.field, s.variant_type, s.rows_
from seen s
left join declared d
  on d.table_name = s.table_name and d.field = s.field and d.variant_type = s.variant_type
where d.field is null

union all

select 'unknown change type', table_name, record:"_change_type"::string, null, count(*)
from {{ source('raw', 'silver_changes') }}
where record:"_change_type"::string not in ('insert', 'update_postimage', 'delete')
   or record:"_change_type" is null
group by 1, 2, 3
