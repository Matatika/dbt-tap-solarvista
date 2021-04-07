-- Assert that all available users in vw_reactive_user_availability_today that have active, scheduled work items 
-- in fact_workitem, and where the scheduled start_time <= now for today are still available.
select
    user_id
from {{ ref('vw_reactive_user_availability_today')}}
where current_availability = 'Available'
and reason notnull
and template_display_name notnull
and not exists (select assigned_user_id
                    from {{ ref('fact_workitem')}}
                    where schedule_start_date = current_date
                    and schedule_start_time <= now()
                    and assigned_user_id = user_id
                    and current_stage not in ('Closed', 'Cancelled', 'RemoteClosed', 'Discarded', 'Rejected', 'Unassigned')
                    and template_display_name != 'Work Order / PPM')