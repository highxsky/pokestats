{% macro stat_values() %}
    {{ return([
        'hp', 
        'attack', 
        'defense', 
        'special-attack', 
        'special-defense', 
        'speed'
    ])}}
{% endmacro %}