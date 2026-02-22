-- test macro to ensure a column is in range

{% macro test_accepted_range(model, column_name, min_value, max_value) %}

    select
    *
    from {{ model }}
    where {{ column_name }} < {{ min_value }}
    and {{ column_name }} > {{ max_value }}

{% endmacro %}