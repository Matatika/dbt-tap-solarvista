{% macro get_stage_name_type(col_type='TIMESTAMP') %}
    {%- set columns = dbt_utils.get_column_values(ref('stg_workitem_stages'), 'stage') | list | sort -%}
    {%- for column in columns %}
        {{ column }} {{ col_type }}
        {%- if not loop.last -%}
        ,
        {%- endif -%}
    {%- endfor %}
{%- endmacro %}