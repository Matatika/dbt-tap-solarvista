-- Assert that no work items in fact_user_assignment that are:
-- closed, cancelled, remoteclosed, discarded, rejected or unassigned and have a assigned_user_id,
-- are missing a to_timestamp
select
    count(*)
from {{ ref('fact_user_assignment')}} as fact_user_assignment
left join {{ ref('fact_workitem')}} as fact_workitem on fact_workitem.work_item_id = fact_user_assignment.work_item_id
where fact_workitem.current_stage in ('Closed', 'Cancelled', 'RemoteClosed', 'Discarded', 'Rejected', 'Unassigned')
and fact_workitem.assigned_user_id notnull
and fact_user_assignment.to_timestamp isnull
having count(*) > 0