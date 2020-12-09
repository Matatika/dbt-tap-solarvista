{{ config(materialized='table') }}

with workitems_hist as (
    select * from {{('cityfm.workitemhistory_stream') }}
),
customers as (
    select * from {{ ref('dim_customer') }}
),
users as (
    select * from {{ ref('dim_user') }}
),
fact_workitem_hist as (
    select distinct 
    workitems_hist.stage_assigned_user_display_name,
    workitems_hist.stage_assigned_user_user_id,
    workitems_hist.stage_stage_display_name,
    workitems_hist.stage_stage_type,
    workitems_hist.stage_transition_from_stage_type,
    workitems_hist.stage_transition_received_at,
    workitems_hist.stage_transition_to_stage_type,
    workitems_hist.stage_transition_transition_id,
    workitems_hist.stage_transition_transitioned_at,
    workitems_hist.work_item_history_id,
    workitems_hist.work_item_id,
    workitems_hist.workflow_id,
    workitems_hist.stage_assigned_user_email,

    --dimensions
    users.users_sk 
    
    from workitems_hist, users
    where users.user_id = workitems_hist.stage_assigned_user_user_id
)
select * from fact_workitem_hist