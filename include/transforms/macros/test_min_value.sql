-- test macro to ensure a column's minimum value

{% macro test_min_value(model, column_name, min_val) %}

    select
    *
    from {{ model }}
    where {{ column_name }} < {{ min_val }}

{% endmacro %}