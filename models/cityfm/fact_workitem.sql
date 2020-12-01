{{ config(materialized='table') }}

with workitems as (
    select * from {{('cityfm.workitem_stream') }}
),
customers as (
    select * from {{ ref('dim_customer') }}
),
users as (
    select * from {{ ref('dim_user') }}
),
fact_workitem as (
    select distinct 
    workitems.assigned_user_name,
    workitems.created_on,
    workitems.current_workflow_stage_display_name,
    workitems.current_workflow_stage_type,
    workitems.properties_charge,
    workitems.properties_duration_hours,
    workitems.properties_price_inc_tax,
    workitems.reference,
    workitems.tags,
    workitems.work_item_id,
    workitems.work_item_template_display_name,
    workitems.work_item_template_id,
    workitems.current_stage_last_transition_id,
    workitems.current_stage_last_transition_time,
    workitems.current_stage_stage_type,
    workitems.fixduedate,
    workitems.is_complete,
    workitems.last_modified,
    workitems.properties_fixduedate,
    workitems.properties_model,
    workitems.properties_responseduedate,
    users.users_sk
from workitems, users
where users.user_id = workitems.assigned_user_id  
)
select * from fact_workitem