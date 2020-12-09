{{ config(materialized='table') }}

with workitems as (
    select * from "{{var('schema')}}".workitem_stream
),
customers as (
    select * from {{ ref('dim_customer') }}
),
users as (
    select * from {{ ref('dim_user') }}
),
fact_workitem as (
    select distinct
    
        -- keys
        workitems.work_item_id,
        workitems.work_item_id as id,
        workitems.reference as reference,
    
        -- dimensions
        workitems.created_on,
        workitems.last_modified,
        workitems.created_on::date as report_date,
        EXTRACT(YEAR FROM created_on)::integer as report_year,
        EXTRACT(MONTH FROM created_on)::integer as report_month,
        EXTRACT(DAY FROM created_on)::integer as report_day,
        workitems.properties_project_id as project_id,
        workitems.properties_territories_id as territory_id,
        workitems.properties_site_id as site_id,
        workitems.properties_customer_id as customer_id,
        workitems.properties_currency_id as currency_id,
        workitems.properties_problem_id as problem_id,
        workitems.properties_equipment_id as equipment_id,
        workitems.assigned_user_id as assigned_user_id,
        workitems.work_item_template_id as template_id,
        workitems.current_workflow_stage_type as current_stage,
        workitems.is_complete,
        workitems.properties_operationalstatus as operationalstatus,
        workitems.properties_model as model,	
        workitems.properties_fixduedate as fixduedate,	
        workitems.properties_responseduedate as responseduedate,	
    
        -- surrogate keys for slowly changing dimensions 
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