{{ config(materialized='table') }}
with workitems as (
    select * from {{ ref('fact_workitem') }}
),

workitem_stages as (
    select * from {{ ref('fact_workitem_stages') }}
),

projects as (
     select distinct * from {{ ref('dim_project') }}
),

dates as (
    select * from {{ ref('dim_date') }}
),

vw_workitem_stages as (
    select distinct 
        workitems.work_item_id,
        workitems.reference,
        projects.reference as project_id,
        projects.createdon,    
        projects.responseduedate,
        projects.fixduedate,

        dates.*,

        workitem_stages.accepted_timestamp as accepted_timestamp,
        workitem_stages.closed_timestamp as closed_timestamp,  
        workitem_stages.assigned_timestamp as assigned_timestamp,  
        workitem_stages.cancelled_timestamp as cancelled_timestamp,  
        workitem_stages.discarded_timestamp as discarded_timestamp,  
        workitem_stages.postworking_timestamp as postworking_timestamp,  
        workitem_stages.preworking_timestamp as preworking_timestamp,  
        workitem_stages.quickclose_timestamp as quickclose_timestamp,  
        workitem_stages.remoteclosed_timestamp as remoteclosed_timestamp,  
        workitem_stages.travellingfrom_timestamp as travellingfrom_timestamp,  
        workitem_stages.travellingto_timestamp as travellingto_timestamp,  
        workitem_stages.unassigned_timestamp as unassigned_timestamp,  
        workitem_stages.working_timestamp as working_timestamp
    from workitems
        left join workitem_stages using (work_item_id)
        left join projects 
            on projects.project_sk = workitems.project_sk
        left outer join dates 
            on dates.date_day = projects.createdon::date
)
select * from vw_workitem_stages 