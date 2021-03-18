{{ config(materialized='view') }}

with fact_user_assignment as (
	select * from {{ ref('fact_user_assignment')}}
),
fact_workitem as (
	select * from {{ ref('fact_workitem')}}
	where schedule_start_date = current_date
),
dim_user as (
	select * from {{ ref('dim_user')}}
),
user_next_scheduled_end_timestamp as (
	select
		distinct(user_id)
		, min(scheduled_end_time) as scheduled_end_time
	from fact_user_assignment
	where fact_user_assignment.from_timestamp::date = current_date
	and fact_user_assignment.from_timestamp <= now()
	and fact_user_assignment.to_timestamp isnull
	group by user_id
),
user_reason_assigned as (
	select
		distinct(fact_user_assignment.user_id) as user_id
		, dim_user.display_name as display_name
		, dim_user.email as email
		, 'Assigned' as current_availability
		, fact_user_assignment.reason as reason
	from fact_user_assignment
	left join dim_user on dim_user.users_sk = fact_user_assignment.user_sk
	where dim_user.is_reactive = true
	and dim_user.is_assignable = true
	and fact_user_assignment.from_timestamp::date = current_date
	and fact_user_assignment.from_timestamp <= now()
	and fact_user_assignment.to_timestamp isnull
	and fact_user_assignment.appointment_id isnull
	and fact_user_assignment.reason not in ('Maintenance', 'Non Productive')
),
user_reason_unavailable as (
	select
		distinct(fact_user_assignment.user_id) as user_id
		, dim_user.display_name as display_name
		, dim_user.email as email
		, 'Unavailable' as current_availability
		, fact_user_assignment.reason as reason
	from fact_user_assignment
	left join dim_user on dim_user.users_sk = fact_user_assignment.user_sk
	where dim_user.is_reactive = true
	and dim_user.is_assignable = true
	and fact_user_assignment.from_timestamp::date = current_date
	and fact_user_assignment.to_timestamp isnull
	and fact_user_assignment.appointment_id notnull
	and fact_user_assignment.user_id not in (select user_id from user_reason_assigned)
),
user_reason_non_productive as (
	select
		distinct(fact_user_assignment.user_id) as user_id
		, dim_user.display_name as display_name
		, dim_user.email as email
		, 'Unavailable' as current_availability
		, fact_user_assignment.reason as reason
	from fact_user_assignment
	left join dim_user on dim_user.users_sk = fact_user_assignment.user_sk
	where dim_user.is_reactive = true
	and dim_user.is_assignable = true
	and fact_user_assignment.from_timestamp::date = current_date
	and fact_user_assignment.from_timestamp <= now()
	and fact_user_assignment.to_timestamp isnull
	and fact_user_assignment.appointment_id isnull
	and fact_user_assignment.reason = 'Non Productive'
	and fact_user_assignment.user_id not in (select user_id from user_reason_assigned
											union
											select user_id from user_reason_unavailable)
),
user_reason_unavailable_due_to_maintenance as (
	select
		distinct(fact_user_assignment.user_id) as user_id
		, dim_user.display_name as display_name
		, dim_user.email as email
		, 'Unavailable' as current_availability
		, fact_user_assignment.reason as reason
	from fact_user_assignment
	left join dim_user on dim_user.users_sk = fact_user_assignment.user_sk
	where dim_user.is_reactive = true
	and dim_user.is_assignable = true
	and fact_user_assignment.from_timestamp <= now()
	and fact_user_assignment.to_timestamp isnull
	and fact_user_assignment.appointment_id isnull
	and fact_user_assignment.reason = 'Maintenance'
	and fact_user_assignment.user_id not in (select user_id from user_reason_assigned
											union
											select user_id from user_reason_unavailable
											union
											select user_id from user_reason_non_productive)
),
user_reason_unassigned as (
	select
		distinct(fact_user_assignment.user_id) as user_id
		, dim_user.display_name as display_name
		, dim_user.email as email
		, 'Unassigned' as current_availability
		, 'Nothing Scheduled Now' as reason
	from fact_user_assignment
	left join dim_user on dim_user.users_sk = fact_user_assignment.user_sk
	where dim_user.is_reactive = true
	and dim_user.is_assignable = true
	and fact_user_assignment.user_id not in (select user_id from user_reason_unavailable_due_to_maintenance
												union
												select user_id from user_reason_non_productive
												union
												select user_id from user_reason_unavailable
												union
												select user_id from user_reason_assigned)
),
union_of_all as (
	select * from user_reason_assigned
	union
	select * from user_reason_unavailable
	union
	select * from user_reason_non_productive
	union
	select * from user_reason_unavailable_due_to_maintenance
	union
	select * from user_reason_unassigned
),
final as (
	select
		union_of_all.user_id as user_id
		, union_of_all.display_name as display_name
		, union_of_all.email as email
		, union_of_all.current_availability as current_availability
		, union_of_all.reason as reason
		, user_next_scheduled_end_timestamp.scheduled_end_time as scheduled_end_time
	from union_of_all
	left join user_next_scheduled_end_timestamp on user_next_scheduled_end_timestamp.user_id = union_of_all.user_id
)
select * from final
order by current_availability