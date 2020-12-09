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
    EXTRACT(YEAR FROM created_on)::integer as report_year,
    EXTRACT(MONTH FROM created_on)::integer as report_month,
    EXTRACT(DAY FROM created_on)::integer as report_day,
    workitems.properties_project_id as project_id,
    workitems.current_workflow_stage_display_name,
    workitems.current_workflow_stage_type as workflow_stage_type,
    workitems.reference,
    workitems.tags,
    workitems.work_item_id,
    workitems.work_item_template_display_name,
    workitems.work_item_template_id,
    workitems.current_stage_last_transition_id,
    workitems.current_stage_last_transition_time,
    workitems.current_stage_stage_type,
    workitems.is_complete,
    workitems.last_modified,
    workitems.properties_fixduedate,
    workitems.properties_model,
    workitems.properties_responseduedate,

    --dimensions 
    users.users_sk, 

    -- metrics
    1 as workitem_count,
    properties_duration_hours as duration_hours,
    properties_charge as charge,
    properties_price_inc_tax as price_inc_tax

    from workitems, users
    where users.user_id = workitems.assigned_user_id
)
select * from fact_workitem