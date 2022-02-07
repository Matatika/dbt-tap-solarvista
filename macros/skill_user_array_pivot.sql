{% macro skill_user_array_pivot() %}
  {{ return(adapter.dispatch('skill_user_array_pivot')()) }}
{% endmacro %}


{% macro default__skill_user_array_pivot() %}

    select 
        value->>'id' "id"
        , array_to_json(array_agg(ss.reference)) "skills_reference"
        , array_to_json(array_agg(ss."name")) "skills_name"
    from skill_table ss, jsonb_array_elements(ss.users)
    right join overall_users on overall_users.user_id = value->>'id'
    where value->>'id' notnull
    group by value->>'id'
    
{% endmacro %}


{# postgres should use default #}
{% macro postgres__skill_user_array_pivot() %}

    {{ return(default__skill_user_array_pivot()) }}

{% endmacro %}



{% macro snowflake__skill_user_array_pivot() %}

    select 
        us.value:"id"::string as id, 
        array_agg(ss.name) as skills_name, 
        array_agg(ss.reference) as skills_reference
    from skill_table as ss, lateral flatten(input => parse_json("USERS")) as us
    group by id

{% endmacro %}