-- Assert that vw_reactive_user_availability_today total unavailable equals total users
-- with appointment and total users 'Non Productive' for today
select
    user_id
from {{ ref('vw_reactive_user_availability_today')}} as vw_reactive_user_availability_today
where current_availability = 'Unavailable'
and not exists (select *
                    from {{ ref('fact_appointment')}} as fact_appointment
                    where "start"::date = current_date
                    and "start" <= now()
                    and "end" > now()
                    and fact_appointment.user_id = vw_reactive_user_availability_today.user_id)
and not exists (select *
                    from {{ ref('fact_workitem')}} as fact_workitem
                    where schedule_start_date = current_date
                    and schedule_start_time <= now()
                    and fact_workitem.assigned_user_id = vw_reactive_user_availability_today.user_id
                    and current_stage not in ('Closed', 'Cancelled', 'RemoteClosed', 'Discarded', 'Rejected', 'Unassigned')
                    and template_display_name = 'Non-Productive Time')