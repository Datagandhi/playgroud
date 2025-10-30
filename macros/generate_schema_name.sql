-- macros/generate_schema_name.sql

{% macro generate_schema_name(custom_schema_name, node) %}
    {{ log("🔧 Using custom generate_schema_name macro. Target name: " ~ target.name, info=True) }}
    {{ custom_schema_name | trim }}
{% endmacro %}