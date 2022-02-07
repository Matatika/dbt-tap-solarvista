{% macro query_array(table_name, column_name, query_string) %}
  {{ return(adapter.dispatch('query_array')(table_name, column_name, query_string)) }}
{% endmacro %}


{% macro default__query_array(table_name, column_name, query_string) %}

    {{table_name}}.{{ column_name }} ? '{{ query_string }}'
    
{% endmacro %}


{# postgres should use default #}
{% macro postgres__query_array(table_name, column_name, query_string) %}

    {{ return(default__query_array(table_name, column_name, query_string)) }}

{% endmacro %}



{% macro snowflake__query_array(table_name, column_name, query_string) %}

    contains({{ column_name }}, '{{ query_string }}')

{% endmacro %}