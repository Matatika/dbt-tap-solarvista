-- Assert that the total count of rows in fact_user_assignment equals count of rows in 
-- fact_appointment and fact_workitems for the last 30 days.
select
    appointment_id
from {{ ref('fact_appointment')}} fact_appointment
where "start"::date >= current_date - interval '30' day
and not exists (
	select
	    *
	from {{ ref('fact_user_assignment')}} fact_user_assignment
	where fact_user_assignment.appointment_id = fact_appointment.appointment_id 
)
union
select
    work_item_id
from {{ ref('fact_workitem') }} fact_workitem
where schedule_start_date >= current_date - interval '30' day
and assigned_user_id notnull
and not exists (
	select
	    *
	from {{ ref('fact_user_assignment')}} fact_user_assignment
	where fact_user_assignment.work_item_id = fact_workitem.work_item_id 
)
union
select
    case when work_item_id is not null then work_item_id else appointment_id end
from {{ ref('fact_user_assignment')}} fact_user_assignment
where from_timestamp >= current_date - interval '7' day
and not exists (
	select
		*
    from {{ ref('fact_appointment')}} fact_appointment
    where fact_appointment.appointment_id = fact_user_assignment.appointment_id
)
and not exists (
	select count(*)
    from {{ ref('fact_workitem') }} fact_workitem
    where assigned_user_id notnull
    and fact_workitem.work_item_id = fact_user_assignment.work_item_id
)

