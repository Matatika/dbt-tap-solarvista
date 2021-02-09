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
sites as (
    select * from {{ ref('dim_site') }}
),
projects as (
    select * from {{ ref('dim_project_snapshot') }} 
    where dbt_valid_to is null
),
territories as (
    select * from {{ ref('dim_territory') }}
),
fact_workitem_incremental as (
    select distinct 

    -- keys
    workitems.work_item_id,
    workitems.work_item_id as id,
    workitems.reference as reference,

    -- time dimensions
    workitems.created_on,
    workitems.last_modified,
    workitems.created_on::date as report_date,
    EXTRACT(YEAR FROM created_on)::integer as report_year,
    EXTRACT(MONTH FROM created_on)::integer as report_month,
    EXTRACT(DAY FROM created_on)::integer as report_day,

    --fact table will only contain SCD surrogate keys
    --below keys needs to be removed from fact and retrieved in reporting views later     
    projects.reference as project_id,
    users.user_id as assigned_user_id,
    territories.reference as territory_id,
    customers.reference as customer_id,

    workitems.properties_site_id as site_id,    
    workitems.properties_currency_id as currency_id,
    workitems.properties_equipment_id as equipment_id,    
    workitems.properties_problem_id as problem_id,
    workitems.work_item_template_id as template_id,

    --all other attributes from workitem stage table
    workitems.current_workflow_stage_type as current_stage,
    workitems.is_complete,
    workitems.tags,
    workitems.properties_operationalstatus as operationalstatus,
    workitems.properties_model as model,	
    workitems.properties_fixduedate as fixduedate,	
    workitems.properties_responseduedate as responseduedate,

    --SCD surrogate keys for join purposes in reporting layer
    users.users_sk, 
    projects.dbt_scd_id as projects_sk,
    territories.territory_sk,
    sites.site_sk,
    customers.customer_sk,

    -- metrics
    1 as workitem_count,
    properties_duration_hours as duration_hours,
    properties_charge as charge,
    properties_price_inc_tax as price_inc_tax

    from workitems, users, projects, territories, customers, sites
    where users.user_id = workitems.assigned_user_id
    and projects.reference = workitems.properties_project_id
    and territories.reference = workitems.properties_territories_id
    and customers.reference = workitems.properties_customer_id
    and sites.reference = workitems.properties_site_id
    
)
select * from fact_workitem_incremental