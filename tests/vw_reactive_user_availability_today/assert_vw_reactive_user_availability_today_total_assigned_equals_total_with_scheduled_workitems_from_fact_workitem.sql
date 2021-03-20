-- Assert that all assigned users in vw_reactive_user_availability_today have active, scheduled work items in fact_workitem,
-- where the scheduled start_time <= now for today
select
    user_id
from {{ ref('vw_reactive_user_availability_today')}}
where current_availability = 'assigned'
and not exists (select assigned_user_id
                    from {{ ref('fact_workitem')}}
                    where schedule_start_date = current_date
                    and schedule_start_time <= now()
                    and assigned_user_id = user_id
                    and current_stage not in ('Closed', 'Cancelled', 'RemoteClosed', 'Discarded', 'Rejected', 'Unassigned')
                    and template_display_name != 'Non-Productive Time')