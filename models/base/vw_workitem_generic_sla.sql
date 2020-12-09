
with workitems_hist as (
    select * from {{ ref('fact_workitem_hist') }}
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
workitems_assigned as (
select distinct * from workitems_hist 
where workitems_hist.stage_stage_type = 'Assigned'
),
workitems_cancelled as (
select distinct * from workitems_hist 
where workitems_hist.stage_stage_type = 'Cancelled'
),
workitems_discarded as (
select distinct * from workitems_hist 
where workitems_hist.stage_stage_type = 'Discarded'
),
workitems_postworking as (
select distinct * from workitems_hist 
where workitems_hist.stage_stage_type = 'PostWorking'
),
workitems_preworking as (
select distinct * from workitems_hist 
where workitems_hist.stage_stage_type = 'PreWorking'
),
workitems_quickclose as (
select distinct * from workitems_hist 
where workitems_hist.stage_stage_type = 'QuickClose'
),
workitems_remoteclosed as (
select distinct * from workitems_hist 
where workitems_hist.stage_stage_type = 'RemoteClosed'
),
workitems_travellingfrom as (
select distinct * from workitems_hist 
where workitems_hist.stage_stage_type = 'TravellingFrom'
),
workitems_travellingto as (
select distinct * from workitems_hist 
where workitems_hist.stage_stage_type = 'TravellingTo'
),
workitems_unassigned as (
select distinct * from workitems_hist 
where workitems_hist.stage_stage_type = 'Unassigned'
),
workitems_working as (
select distinct * from workitems_hist 
where workitems_hist.stage_stage_type = 'Working'
),
vw_workitem_generic_sla as (
    select distinct 
    workitems.work_item_id,
    projects.reference,
    projects.createdon,    
    projects.responseduedate,
    projects.fixduedate,
    workitems_accepted.stage_transition_received_at as workitem_accepted_received_at,    
    workitems_closed.stage_transition_received_at as workitem_closed_received_at,  
    workitems_assigned.stage_transition_received_at as workitem_assigned_received_at,  
    workitems_cancelled.stage_transition_received_at as workitem_cancelled_received_at,  
    workitems_discarded.stage_transition_received_at as workitem_discarded_received_at,  
    workitems_postworking.stage_transition_received_at as workitem_postworking_received_at,  
    workitems_preworking.stage_transition_received_at as workitem_preworking_received_at,  
    workitems_quickclose.stage_transition_received_at as workitem_quickclose_received_at,  
    workitems_remoteclosed.stage_transition_received_at as workitem_remoteclosed_received_at,  
    workitems_travellingfrom.stage_transition_received_at as workitem_travellingfrom_received_at,  
    workitems_travellingto.stage_transition_received_at as workitem_travellingto_received_at,  
    workitems_unassigned.stage_transition_received_at as workitem_unassigned_received_at,  
    workitems_working.stage_transition_received_at as workitem_working_received_at    
from workitems
left join projects 
on projects.reference = workitems.project_id
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
select * from vw_workitem_generic_sla