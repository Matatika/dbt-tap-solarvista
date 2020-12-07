{% macro get_stage_name_value() %}
    {%- set columns = dbt_utils.get_column_values(ref('stg_workitem_stages'), 'stage') | list | sort -%}
    {%- for column in columns %}
        ('{{ column }}')
        {%- if not loop.last -%}
        ,
        {%- endif -%}
    {%- endfor %}
{%- endmacro %}