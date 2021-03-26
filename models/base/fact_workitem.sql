--
-- work items facts
--

{{
    config(
        materialized='incremental',
        unique_key='work_item_id'
    )
}}

with workitems as (
    select * from "{{var('schema')}}".workitem_stream
),
project_stream as (
    select * from "{{var('schema')}}".project_stream
),
customers as (
    select * from {{ ref('dim_customer') }}
),
sites as (
    select * from {{ ref('dim_site') }}
),
users as (
    select * from {{ ref('dim_user') }}
),
projects as (
    select * from {{ ref('dim_project') }}
),
territories as (
    select * from {{ ref('dim_territory') }}
),
dates as (
    select * from {{ ref('dim_date') }}
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
        workitems.work_item_template_display_name as template_display_name,

        --all other attributes from workitem stage table
        workitems.current_workflow_stage_type as current_stage,
        workitems.is_completed,
        workitems.tags,
        workitems.schedule_start_time::date as schedule_start_date,	
        workitems.schedule_start_time,	
        workitems.schedule_duration_minutes,	
        workitems.schedule_travel_time_minutes,
        workitems.properties_operationalstatus as operationalstatus,
        workitems.properties_model as model,
        workitems.properties_source as source,
        workitems.properties_fixduedate as fixduedate,	
        workitems.properties_responseduedate as responseduedate,	    
        
        --SCD surrogate keys for join purposes in reporting layer
        users.users_sk, 
        projects.project_sk as project_sk,
        territories.territory_sk,
        sites.site_sk,
        customers.customer_sk,

        -- metrics
        1 as workitem_count,
        properties_duration_hours as duration_hours,
        properties_charge as charge,
        properties_price_inc_tax as price_inc_tax

    from workitems
	left join users on users.user_id = workitems.assigned_user_id
	left join projects on projects.reference = workitems.properties_project_id
    left join territories on territories.reference = workitems.properties_territories_id
    left join sites on sites.reference = workitems.properties_site_id
    left join customers on customers.reference = workitems.properties_customer_id
	left join project_stream on project_stream.reference = workitems.properties_project_id
    where (
        workitems.properties_project_id is null -- load work items without projects (ad hoc work items)
        or project_stream.status != 'Discarded'  -- do not load work items from discarded projects
    )
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    and workitems.last_modified >= (select max(t2.last_modified) from {{ this }} as t2)
{% endif %}
)
select * from fact_workitem