-- Assert that vw_reactive_user_availability_today total unavailable equals total users
-- with appointment and total users 'Non Productive' for today
select
    count(*)
from {{ ref('vw_reactive_user_availability_today')}}
where current_availability = 'Unavailable'
having count(*) != (select count(*)
                    from {{ ref('fact_appointment')}}
                    where "start"::date = current_date
                    and "start" <= now()
                    and "end" > now()) +
                    (select count(*)
                    from {{ ref('fact_workitem')}}
                    where schedule_start_date = current_date
                    and schedule_start_time <= now()
                    and assigned_user_id notnull
                    and current_stage not in ('Closed', 'Cancelled', 'RemoteClosed', 'Discarded', 'Rejected', 'Unassigned')
                    and template_display_name = 'Non-Productive Time')