
with workitems_hist as (
    select * from {{ ref('fact_workitem_history') }}
),
workitems as (
    select * from {{ ref('fact_workitem') }}
),
projects as (
     select distinct * from {{ ref('dim_project') }}
),
workitems_accepted as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_hist ht 
    where ht.stage_stage_type = 'Accepted' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_hist ht ON ht.work_item_id = t2.work_item_id AND ht.received_at= t2.MinDate
),
workitems_closed as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_hist ht 
    where ht.stage_stage_type = 'Closed' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_hist ht ON ht.work_item_id = t2.work_item_id AND ht.received_at= t2.MinDate
),
workitems_assigned as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_hist ht 
    where ht.stage_stage_type = 'Assigned' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_hist ht ON ht.work_item_id = t2.work_item_id AND ht.received_at= t2.MinDate
),
workitems_cancelled as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_hist ht 
    where ht.stage_stage_type = 'Cancelled' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_hist ht ON ht.work_item_id = t2.work_item_id AND ht.received_at= t2.MinDate
),
workitems_discarded as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_hist ht 
    where ht.stage_stage_type = 'Discarded' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_hist ht ON ht.work_item_id = t2.work_item_id AND ht.received_at= t2.MinDate
),
workitems_postworking as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_hist ht 
    where ht.stage_stage_type = 'PostWorking' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_hist ht ON ht.work_item_id = t2.work_item_id AND ht.received_at= t2.MinDate
),
workitems_preworking as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_hist ht 
    where ht.stage_stage_type = 'PreWorking' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_hist ht ON ht.work_item_id = t2.work_item_id AND ht.received_at= t2.MinDate
),
workitems_quickclose as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_hist ht 
    where ht.stage_stage_type = 'QuickClose' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_hist ht ON ht.work_item_id = t2.work_item_id AND ht.received_at= t2.MinDate
),
workitems_remoteclosed as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_hist ht 
    where ht.stage_stage_type = 'RemoteClosed' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_hist ht ON ht.work_item_id = t2.work_item_id AND ht.received_at= t2.MinDate
),
workitems_travellingfrom as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_hist ht 
    where ht.stage_stage_type = 'TravellingFrom' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_hist ht ON ht.work_item_id = t2.work_item_id AND ht.received_at= t2.MinDate
),
workitems_travellingto as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_hist ht 
    where ht.stage_stage_type = 'TravellingTo' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_hist ht ON ht.work_item_id = t2.work_item_id AND ht.received_at= t2.MinDate
),
workitems_unassigned as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_hist ht 
    where ht.stage_stage_type = 'Unassigned' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_hist ht ON ht.work_item_id = t2.work_item_id AND ht.received_at= t2.MinDate
),
workitems_working as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_hist ht 
    where ht.stage_stage_type = 'Working' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_hist ht ON ht.work_item_id = t2.work_item_id AND ht.received_at= t2.MinDate
),
vw_workitem_stages as (
    select distinct 
    workitems.work_item_id,
    projects.reference,
    projects.createdon,    
    projects.responseduedate,
    projects.fixduedate,
    workitems_accepted.received_at as accepted,
    workitems_closed.received_at as closed,  
    workitems_assigned.received_at as assigned,  
    workitems_cancelled.received_at as cancelled,  
    workitems_discarded.received_at as discarded,  
    workitems_postworking.received_at as postworking,  
    workitems_preworking.received_at as preworking,  
    workitems_quickclose.received_at as quickclose,  
    workitems_remoteclosed.received_at as remoteclosed,  
    workitems_travellingfrom.received_at as travellingfrom,  
    workitems_travellingto.received_at as travellingto,  
    workitems_unassigned.received_at as unassigned,  
    workitems_working.received_at as working    
from workitems
left join projects 
on projects.project_sk = workitems.project_sk
left join workitems_accepted using (work_item_id)
left join workitems_closed using (work_item_id)
left join workitems_assigned using (work_item_id)
left join workitems_cancelled using (work_item_id)
left join workitems_discarded using (work_item_id)
left join workitems_postworking using (work_item_id)
left join workitems_preworking using (work_item_id)
left join workitems_quickclose using (work_item_id)
left join workitems_remoteclosed using (work_item_id)
left join workitems_travellingfrom using (work_item_id)
left join workitems_travellingto using (work_item_id) 
left join workitems_unassigned using (work_item_id)
left join workitems_working using (work_item_id)
)
select * from vw_workitem_stages 