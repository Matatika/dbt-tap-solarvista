{{ config(materialized='table') }}

with users as (
    select * from "{{var('schema')}}".users_stream
),
skill_stream as (
    select * from "{{var('schema')}}".skill_stream
),
users_with_skills as (
    select 
        value->>'id' "skilled_user_id"
        , array_to_json(array_agg(ss.reference)) "skills_reference"
        , array_to_json(array_agg(ss."name")) "skills_name"
    from skill_stream ss, jsonb_array_elements(ss.users)
    right join users on users.user_id = value->>'id'
    where value->>'id' notnull
    group by value->>'id'
),
dim_user as (
    select
        {{ dbt_utils.surrogate_key(['user_id']) }} as users_sk
        , users.*
        , uws.skills_name
        , uws.skills_reference
    from users
    left join users_with_skills uws on uws.skilled_user_id = users.user_id
)
select * from dim_user
