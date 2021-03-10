{{ config(materialized='table') }}

with users as (
    select * from "{{var('schema')}}".users_stream
),
skill_stream as (
    select * from "{{var('schema')}}".skill_stream
),
reactive_engineers as (
    select
    id
    from skill_stream, jsonb_to_recordset(users) as users_skills(id text)
    where name = 'Reactive'
),
maintenance_engineers as (
    select
    id
    from skill_stream, jsonb_to_recordset(users) as users_skills(id text)
    where name = 'Maintenance'
),
users_with_engineer_types as (
	select 
	users.display_name as display_name
	, users.email as email
    , users.user_id as user_id
    , users.is_assignable as is_assignable
	, case when users.user_id in (select * from reactive_engineers) then true else false end as is_reactive
    , case when users.user_id in (select * from maintenance_engineers) then true else false end as is_maintenance
	from users
),
dim_user as (
    select
        {{ dbt_utils.surrogate_key(['user_id']) }} as users_sk,
        *
    from users_with_engineer_types
)
select * from dim_user
