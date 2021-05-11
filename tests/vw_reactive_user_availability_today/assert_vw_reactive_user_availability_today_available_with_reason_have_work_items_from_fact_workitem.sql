-- Assert that all available users in vw_reactive_user_availability_today that have active, scheduled work items 
-- in fact_workitem, and where the scheduled start_time <= now for today are still available.

-- Edge case causing this test to fail very occasionally
-- Sometime an engineer's only active jobs are ones that are assigned to them a previous day.
-- This means they are assigned, and are working, but this test thinks they shouldnt be as it only checks when the 
-- scheduled_start_date = current_date
select
    user_id
from {{ ref('vw_reactive_user_availability_today')}}
where current_availability = 'Available'
and reason notnull
and template_display_name notnull
and not exists (select assigned_user_id
                    from {{ ref('fact_workitem')}}
                    left join {{ ref('fact_workitem_stages')}} on fact_workitem_stages.work_item_id = fact_workitem.work_item_id
                    where schedule_start_date = current_date
                    and (schedule_start_time <= now() AT TIME ZONE 'BST' or fact_workitem_stages.accepted_timestamp <= now() AT TIME ZONE 'BST')
                    and assigned_user_id = user_id
                    and current_stage not in ('Closed', 'Cancelled', 'RemoteClosed', 'Discarded', 'Rejected', 'Unassigned')
                    and template_display_name != 'Work Order / PPM')