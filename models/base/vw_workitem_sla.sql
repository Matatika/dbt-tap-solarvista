
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
select distinct * from workitems_hist 
where workitems_hist.stage_stage_type = 'Accepted'
),
workitems_closed as (
select distinct * from workitems_hist 
where workitems_hist.stage_stage_type = 'Closed'
),
vw_workitem_sla as (
    select distinct 
    workitems.work_item_id,
    projects.reference,
    projects.createdon,    
    projects.responseduedate,
    projects.fixduedate,
    workitems_accepted.received_at as accepted,    
    workitems_closed.received_at as closed,    
    --To compute SLA response time comparing project responseduedate with workitem accepted date
    DATE_PART('day',projects.responseduedate::timestamp - workitems_accepted.received_at::timestamp) * 24 +
    DATE_PART('hour',projects.responseduedate::timestamp - workitems_accepted.received_at::timestamp) as responsetime,  
    --To compute SLA fix time comparing project fixduedate with workitem closed transition date
    DATE_PART('day',workitems_closed.received_at::timestamp - projects.fixduedate::timestamp) * 24 +
    DATE_PART('hour',workitems_closed.received_at::timestamp - projects.fixduedate::timestamp) as fixtime,           
    --To compute actual SLA response time comparing project created on with workitem closed transition date
    DATE_PART('day',workitems_closed.received_at::timestamp - projects.createdon::timestamp) * 24 +
    DATE_PART('hour',workitems_closed.received_at::timestamp - projects.createdon::timestamp) as actualresponse           
from projects,workitems,workitems_accepted, workitems_closed
where projects.project_sk = workitems.project_sk
and workitems.work_item_id = workitems_accepted.work_item_id 
and workitems.work_item_id = workitems_closed.work_item_id
and workitems_accepted.work_item_id = workitems_closed.work_item_id
)
select * from vw_workitem_sla