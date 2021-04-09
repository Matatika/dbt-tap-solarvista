{{ config(materialized='view') }}

with fact_user_assignment as (
	select * from {{ ref('fact_user_assignment')}}
),
dim_user as (
	select * from {{ ref('dim_user')}}
),
reactive_assignable_users as (
	select
		*
	from dim_user
	where is_assignable = true
	and is_reactive = true
),
current_reactive_user_assignments as (
    select
        fact_user_assignment.user_sk as user_sk
		, fact_user_assignment.user_id as user_id
		, fact_user_assignment.from_timestamp as from_timestamp
		, fact_user_assignment.to_timestamp as to_timestamp
		, fact_user_assignment.appointment_id as appointment_id
		, fact_user_assignment.work_item_id as work_item_id
		, fact_user_assignment.template_display_name as template_display_name
		, fact_user_assignment.reason as reason
		, fact_user_assignment.scheduled_to_time as scheduled_to_time
		, dim_user.display_name as display_name
		, dim_user.email as email
    from fact_user_assignment
	left join dim_user on dim_user.users_sk = fact_user_assignment.user_sk
    where from_timestamp::date = current_date
    and from_timestamp <= now() AT TIME ZONE 'BST'
    and to_timestamp isnull
    and fact_user_assignment.user_id in (select reactive_assignable_users.user_id from reactive_assignable_users)
),
next_available as (
	-- Selects distinct user based on the outer join where we get the maximum scheduled_to_time
	-- (We outer join so we have to "where" b.user_id isnull)
	select
		distinct on (a.user_sk) a.user_sk
		, a.user_id as user_id
		, a.from_timestamp as from_timestamp
		, a.to_timestamp as to_timestamp
		, a.appointment_id as appointment_id
		, a.work_item_id as work_item_id
		, a.template_display_name as template_display_name
		, a.reason as reason
		, a.scheduled_to_time as scheduled_to_time
		, a.display_name as display_name
		, a.email as email
	from current_reactive_user_assignments a
	left outer join current_reactive_user_assignments b 
		on a.user_id = b.user_id 
		and a.scheduled_to_time < b.scheduled_to_time
	where b.user_id isnull
),
user_reason_assigned as (
	select
		next_available.user_id as user_id
		, next_available.display_name as display_name
		, next_available.email as email
		, 'Available' as current_availability
		, next_available.reason as reason
		, next_available.template_display_name as template_display_name
		, next_available.scheduled_to_time as scheduled_to_time
	from next_available
	where next_available.appointment_id isnull
	and next_available.reason not in ('Maintenance')
),
user_reason_unavailable as (
	select
		next_available.user_id as user_id
		, next_available.display_name as display_name
		, next_available.email as email
		, 'Unavailable' as current_availability
		, 'Appointment' as reason
		, next_available.reason as template_display_name
		, next_available.scheduled_to_time as scheduled_to_time
	from next_available
	where next_available.appointment_id notnull
),
user_reason_unavailable_due_to_maintenance as (
	select
		next_available.user_id as user_id
		, next_available.display_name as display_name
		, next_available.email as email
		, 'Unavailable' as current_availability
		, next_available.reason as reason
		, next_available.template_display_name as template_display_name
		, next_available.scheduled_to_time as scheduled_to_time
	from next_available
	where next_available.appointment_id isnull
	and next_available.reason = 'Maintenance'
),
user_reason_unassigned as (
	select
		reactive_assignable_users.user_id as user_id
		, reactive_assignable_users.display_name as display_name
		, reactive_assignable_users.email as email
		, 'Available' as current_availability
		, NULL as reason
		, NULL as template_display_name
		, NULL::timestamp as scheduled_to_time
	from reactive_assignable_users
	where reactive_assignable_users.user_id not in (select user_id from user_reason_unavailable_due_to_maintenance
												union
												select user_id from user_reason_unavailable
												union
												select user_id from user_reason_assigned)
),
final as (
	select * from user_reason_assigned
	union
	select * from user_reason_unavailable
	union
	select * from user_reason_unavailable_due_to_maintenance
	union
	select * from user_reason_unassigned
)
select * from final
order by current_availability