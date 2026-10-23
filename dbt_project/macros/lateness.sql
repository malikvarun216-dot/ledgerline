{#- How late an order was, in six named buckets, from days_late (delivered date minus promised date;
    negative = early). One definition for every mart and report (Session 22). -#}

{% macro lateness_bucket(days_late) -%}
    case
        when {{ days_late }} <= -8 then 'early 8+ days'
        when {{ days_late }} <= -1 then 'early 1-7 days'
        when {{ days_late }} = 0   then 'on the promised day'
        when {{ days_late }} <= 3  then 'late 1-3 days'
        when {{ days_late }} <= 7  then 'late 4-7 days'
        else 'late 8+ days'
    end
{%- endmacro %}

{#- The buckets' order for a chart axis: 1 = earliest. -#}
{% macro lateness_bucket_order(days_late) -%}
    case
        when {{ days_late }} <= -8 then 1
        when {{ days_late }} <= -1 then 2
        when {{ days_late }} = 0   then 3
        when {{ days_late }} <= 3  then 4
        when {{ days_late }} <= 7  then 5
        else 6
    end
{%- endmacro %}
