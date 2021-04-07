-- Given users with appointments for today
-- Given users with 'Non Productive' work for today
-- Given users with 'Maintenance' assigned for today
-- Expect only these users to be Unavailable in vw_reactive_user_availability_today
-- Assert that vw_reactive_user_availability_today return no other results
select
    user_id
from {{ ref('vw_reactive_user_availability_today_old')}} as vw_reactive_user_availability_today
where current_availability = 'Unavailable'
and not exists (select *
                    from {{ ref('fact_appointment')}} as fact_appointment
                    where "start"::date = current_date
                    and "start" <= now()
--                    and "end" > now()
                    and fact_appointment.user_id = vw_reactive_user_availability_today.user_id)
and not exists (select *
                    from {{ ref('fact_workitem')}} as fact_workitem
                    where schedule_start_date = current_date
                    and schedule_start_time <= now()
                    and fact_workitem.assigned_user_id = vw_reactive_user_availability_today.user_id
                    and current_stage not in ('Closed', 'Cancelled', 'RemoteClosed', 'Discarded', 'Rejected', 'Unassigned')
                    and template_display_name = 'Non-Productive Time')
and not exists (select *
                    from {{ ref('fact_user_assignment')}}
                    where to_timestamp is null
                    and fact_user_assignment.from_timestamp::date = current_date
                    and fact_user_assignment.from_timestamp <= now()
                    and fact_user_assignment.user_id = vw_reactive_user_availability_today.user_id
                    and reason in ('Maintenance', 'Non Productive'))