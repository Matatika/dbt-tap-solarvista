{{ config(materialized='table') }}

--
-- This view is able to answer questions related to how users acted on workitems.
-- e.g. Did a user closed a given work item?
-- How many work items did a user work on?
--
with workitem_facts as (
    select * from {{ ref('fact_workitem') }}
),

workitems_history as (
    select * from {{ ref('fact_workitem_history') }}
),

workitems_preworking as (
    -- Sql to retrieve the work item attended stage by a specific user
    select
        work_item_id, stage_transition_transitioned_by_user_id, stage_transition_received_at
    from workitems_history
    where stage_transition_to_stage_type = 'PreWorking'
    and stage_transition_transitioned_by_user_id is not null
),

workitems_remoteclosed as (
    -- Sql to retrieve the work item remote closed stage by a specific user
    select
        work_item_id, stage_transition_transitioned_by_user_id, stage_transition_received_at
    from workitems_history
    where stage_transition_to_stage_type = 'RemoteClosed'
    and stage_transition_transitioned_by_user_id is not null
),

assigned_users as (
    select * from {{ ref('dim_user') }}
),

attended_users as (
    select * from {{ ref('dim_user') }}
),

remoteclosed_users as (
    select * from {{ ref('dim_user') }}
),

customers as (
     select distinct * from {{ ref('dim_customer') }}
),

sites as (
     select distinct * from {{ ref('dim_site') }}
),

territories as (
     select distinct * from {{ ref('dim_territory') }}
),

dates as (
    select * from {{ ref('dim_date') }}
),

stats as (
    select
        workitem_facts.work_item_id
        ,assigned_user_id
        ,created_on::date as report_date
        ,EXTRACT(YEAR FROM created_on)::integer as report_year
        ,EXTRACT(MONTH FROM created_on)::integer as report_month
        ,EXTRACT(DAY FROM created_on)::integer as report_day

        -- keys
        ,territory_sk
        ,site_sk
        ,customer_sk
        
        -- users who performed each stage
        ,workitems_preworking.stage_transition_transitioned_by_user_id as preworking_by_user_id
        ,workitems_preworking.stage_transition_received_at as preworking_timestamp
        ,workitems_remoteclosed.stage_transition_transitioned_by_user_id as remoteclosed_by_user_id
        ,workitems_remoteclosed.stage_transition_received_at as remoteclosed_timestamp

        --
        -- metrics
        --
        ,workitem_facts.workitem_count as workitem_count
        ,workitem_facts.duration_hours as duration_hours
    from workitem_facts
    left join workitems_preworking on workitems_preworking.work_item_id = workitem_facts.work_item_id
    left join workitems_remoteclosed on workitems_remoteclosed.work_item_id = workitem_facts.work_item_id
),

final as (
    select
        work_item_id
        ,report_date
        ,report_year
        ,report_month
        ,report_day

        -- dimensions
        ,dates.day_of_month
        ,dates.day_of_year
        ,dates.day_of_week
        ,dates.day_of_week_name
        ,territories.reference as territory_id
        ,territories.name as territory_name
        ,sites.reference as site_id
        ,sites.name as site_name
        ,customers.reference as customer_id
        ,customers.name as customer_name
        ,assigned_users.user_id as assigned_user_id
        ,assigned_users.display_name as assigned_user_name
        ,attended_users.user_id as attended_user_id
        ,attended_users.display_name as attended_user_name
        ,preworking_timestamp as attended_timestamp
        ,remoteclosed_users.user_id as remoteclosed_user_id
        ,remoteclosed_users.display_name as remoteclosed_user_name
        ,remoteclosed_timestamp

        --
        -- metrics
        --
        ,workitem_count
        ,duration_hours

    from stats
        left join sites on sites.site_sk = stats.site_sk
        left join territories on territories.territory_sk = stats.territory_sk
        left join customers on customers.customer_sk = stats.customer_sk
        left join assigned_users on assigned_users.user_id = stats.assigned_user_id
        left join attended_users on attended_users.user_id = stats.preworking_by_user_id
        left join remoteclosed_users on remoteclosed_users.user_id = stats.remoteclosed_by_user_id
        left join dates on dates.date_day = stats.report_date
)
select * from final