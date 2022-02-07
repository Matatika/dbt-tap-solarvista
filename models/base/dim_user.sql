{{ config(materialized='table') }}

with user_snapshot as (
    select * from {{ ref('dim_user_snapshot') }}
),
skill_stream as (
    select * from {{ source ('solarvista_source', 'skill_stream') }}
),
active_users as (
    select
        *
    from user_snapshot
    where dbt_valid_to is null 
),
deleted_users as (
    select
	    *
    from user_snapshot us
    where not exists (select *
                    from  user_snapshot us2
                    where us2.user_id = us.user_id
                    and us2.dbt_valid_to > us.dbt_valid_to)
    and user_id not in (
    select
        user_id 
    from active_users )
    and dbt_valid_to notnull
),
overall_users as (
    select
        *
    from active_users
    union
    select
        *
    from deleted_users
),
users_with_skills as (
    select 
        value->>'id' "skilled_user_id"
        , array_to_json(array_agg(ss.reference)) "skills_reference"
        , array_to_json(array_agg(ss."name")) "skills_name"
    from skill_stream ss, jsonb_array_elements(ss.users)
    right join overall_users on overall_users.user_id = value->>'id'
    where value->>'id' notnull
    group by value->>'id'
),
dim_user as (
    select
        {{ dbt_utils.surrogate_key(['user_id']) }} as users_sk
        , overall_users.user_id as user_id
        , overall_users.display_name as display_name
        , overall_users.email as email
        , overall_users.is_assignable as is_assignable
        , case WHEN overall_users.dbt_valid_to notnull then True else False end as is_deleted
        , uws.skills_name
        , uws.skills_reference
    from overall_users
    left join users_with_skills uws on uws.skilled_user_id = overall_users.user_id
)
select * from dim_user
