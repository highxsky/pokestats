{% macro mart_incremental_load_config(unique_key, source_ref) %}
{{
    config(
        materialized='incremental', 
        unique_key=unique_key,
        incremental_strategy='delete+insert',
        pre_hook="SET variable max_fetch_date = (SELECT MAX(fetch_date) FROM {{ ref('" ~ source_ref ~ "') }});"
    )
}}
{% endmacro %}