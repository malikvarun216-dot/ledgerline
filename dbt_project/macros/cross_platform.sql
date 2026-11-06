{#-
  The few SQL spellings that differ between Snowflake and Databricks (Session 28, the `databricks`
  target). Each macro is called by its plain name and dispatches on the adapter:
  `adapter.dispatch('name')` picks `snowflake__name` or `databricks__name`, else `default__name`.
  The Snowflake versions are the SQL the models held before S28, character for character, so the
  primary target's compiled SQL does not change.

  Every one of these was found by a probe on the Serverless Starter Warehouse (constraints.md, dbt):
    vget          `record:"field"` is refused; Databricks reads a VARIANT field as `record:['field']`
    dec           NUMBER(p, s) is refused; DECIMAL(p, s) is the same type
    vhash         hash() refuses a VARIANT; hash its JSON text instead
    trim_space    '[[:space:]]' is not a whitespace class in Java regex: it trims NOTHING, silently
    rlike_dotall  regexp_like takes no third ('s' = dot matches newline) argument; `(?s)` inline
-#}

{% macro vget(column, field) -%}{{ return(adapter.dispatch('vget')(column, field)) }}{%- endmacro %}
{% macro default__vget(column, field) -%}{{ column }}:"{{ field }}"{%- endmacro %}
{% macro databricks__vget(column, field) -%}{{ column }}:['{{ field }}']{%- endmacro %}


{% macro dec(precision, scale) -%}{{ return(adapter.dispatch('dec')(precision, scale)) }}{%- endmacro %}
{% macro default__dec(precision, scale) -%}number({{ precision }}, {{ scale }}){%- endmacro %}
{% macro databricks__dec(precision, scale) -%}decimal({{ precision }}, {{ scale }}){%- endmacro %}


{#- Only ever compared with itself on one platform (no_conflicting_changes): the two platforms'
    values differ, and nothing compares them. -#}
{% macro vhash(column) -%}{{ return(adapter.dispatch('vhash')(column)) }}{%- endmacro %}
{% macro default__vhash(column) -%}hash({{ column }}){%- endmacro %}
{% macro databricks__vhash(column) -%}xxhash64(to_json({{ column }})){%- endmacro %}


{#- Leading and trailing whitespace off. POSIX [[:space:]] and Java's \s are the same six
    characters (space, tab, newline, vertical tab, form feed, carriage return). -#}
{% macro trim_space(expr) -%}{{ return(adapter.dispatch('trim_space')(expr)) }}{%- endmacro %}
{% macro default__trim_space(expr) -%}regexp_replace({{ expr }}, '^[[:space:]]+|[[:space:]]+$', ''){%- endmacro %}
{% macro databricks__trim_space(expr) -%}regexp_replace({{ expr }}, '^\\s+|\\s+$', ''){%- endmacro %}


{#- True when the WHOLE string matches (Snowflake's regexp_like is anchored at both ends; Java's
    rlike finds the pattern anywhere — our patterns start and end with `.*`, so both agree), with
    `.` also matching a newline. -#}
{% macro rlike_dotall(expr, pattern) -%}{{ return(adapter.dispatch('rlike_dotall')(expr, pattern)) }}{%- endmacro %}
{% macro default__rlike_dotall(expr, pattern) -%}regexp_like({{ expr }}, '{{ pattern }}', 's'){%- endmacro %}
{% macro databricks__rlike_dotall(expr, pattern) -%}{{ expr }} rlike '(?s)^{{ pattern }}$'{%- endmacro %}


{#- A ratio to 4 places, rounded ONCE (S28). Snowflake divides two integers into a NUMBER with 6
    places, so round(a / b, 4) rounds twice: 902 / 203 = 4.44334975 -> 4.443350 -> 4.4434, not 4.4433.
    Dividing a NUMBER(38, 12) keeps 12 places, and the one rounding is to 4. Databricks divides
    integers into a DOUBLE, which already holds the digits. -#}
{% macro ratio(numerator, denominator) -%}{{ return(adapter.dispatch('ratio')(numerator, denominator)) }}{%- endmacro %}
{% macro default__ratio(numerator, denominator) -%}round(cast({{ numerator }} as number(38, 12)) / nullif({{ denominator }}, 0), 4){%- endmacro %}
{% macro databricks__ratio(numerator, denominator) -%}round({{ numerator }} / nullif({{ denominator }}, 0), 4){%- endmacro %}
