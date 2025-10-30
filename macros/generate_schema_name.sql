-- macros/generate_schema_name.sql

{% macro generate_schema_name(custom_schema_name, node) %}
    {% set target_name = target.name %}
    {{ log("🔧 Using custom generate_schema_name macro. Target name: " ~ target_name, info=True) }}
    
    {% if target_name == 'PEAKAI' %}
        {{ custom_schema_name | trim }}
    {% else %}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {% endif %}
{% endmacro %}