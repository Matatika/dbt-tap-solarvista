{{ config(materialized='table') }}

with dim_user as (
    select * from {{ ref('dim_user') }}
),
fact_workitem as (
    select * from {{ ref('fact_workitem') }}
),
fact_appointment as (
    select * from {{ ref('fact_appointment') }}
),
assigned_engineers as (
	select 
		distinct(user_id)
	from fact_workitem
	left join dim_user as assigned_users on assigned_users.users_sk = fact_workitem.users_sk
	where schedule_start_time::date = current_date
	and user_id is not null
),
engineers_with_appointments as (
	select
		distinct(user_id)
	from fact_appointment
	where "start"::date = current_date
),
non_productive_engineers as (
	select
		distinct(fact_workitem.assigned_user_id)
	from fact_workitem 
	where fact_workitem.schedule_start_date::date = current_date
	and fact_workitem.current_stage != 'Closed'
	and fact_workitem.assigned_user_id notnull
	and fact_workitem.template_display_name = 'Non-Productive Time'
),
assignable_users as (
	select 
		user_id
	from dim_user
	where is_assignable = true
),
not_assignable_users as (
	select 
		user_id
	from dim_user
	where is_assignable = false
),
engineer_availability as (
	select 
	user_id
	, display_name
	, email
	, is_reactive
	, is_maintenance
	, case when user_id in (select * 
						from assignable_users
						where user_id not in (
							select * from assigned_engineers
						union
							select * from engineers_with_appointments
						union
							select * from non_productive_engineers)) then 'Unassigned'
			when user_id in (select * from assigned_engineers
						where user_id not in (
							select * from non_productive_engineers)) then 'Assigned'
			when user_id in (select * from engineers_with_appointments
						where user_id not in (
							select * from not_assignable_users)
						union
							select * from non_productive_engineers) 
							then 'Unavailable' end as current_availability
	from dim_user
	where is_assignable = true
),
final as (
	select
		current_date as availability_date
		, engineer_availability.user_id as user_id
		, engineer_availability.display_name as display_name
		, engineer_availability.email as email
		, engineer_availability.is_reactive as is_reactive
		, engineer_availability.is_maintenance as is_maintenance
		, engineer_availability.current_availability as current_availability
    from engineer_availability
)
select * from final