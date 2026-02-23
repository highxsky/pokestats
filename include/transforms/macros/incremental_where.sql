{% macro incremental_where(column='fetch_date') %}
    {% if is_incremental() %}
    WHERE fetch_date >= getvariable('max_fetch_date')
    {% endif %}
{% endmacro %}