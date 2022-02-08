{{ config(materialized='table') }}

with user_snapshot as (
    select * from {{ ref('dim_user_snapshot') }}
),
skill_table as (
    select * from {{ ref('dim_skill') }}
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
    and dbt_valid_to is not null
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
    {{ skill_user_array_pivot() }}
),
dim_user as (
    select
        {{ dbt_utils.surrogate_key(['user_id']) }} as users_sk
        , overall_users.user_id as user_id
        , overall_users.display_name as display_name
        , overall_users.email as email
        , overall_users.is_assignable as is_assignable
        , case WHEN overall_users.dbt_valid_to is not null then True else False end as is_deleted
        , uws.skills_name
        , uws.skills_reference
    from overall_users
    left join users_with_skills uws on uws.id = overall_users.user_id
)
select * from dim_user
