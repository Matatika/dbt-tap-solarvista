{{ config(materialized='table') }}

with workitemhistory as (
    select * from "{{var('schema')}}".workitemhistory_stream
),
users as (
    select * from {{ ref('dim_user') }}
),
fact_workitem_history as (
    select distinct 
        -- key transforms
        workitemhistory.work_item_history_id as id,

	    -- dimensions
        users.users_sk, 
        EXTRACT(YEAR FROM stage_transition_received_at)::integer as report_year,
        EXTRACT(MONTH FROM stage_transition_received_at)::integer as report_month,
        EXTRACT(DAY FROM stage_transition_received_at)::integer as report_day,

        -- metrics

        -- add everything else; --all stage transition columns from workitem history stage table
    	workitemhistory.stage_stage_type as stage,
        workitemhistory.stage_transition_received_at as received_at,
        workitemhistory.stage_transition_received_at::date as report_date,

        workitemhistory.stage_assigned_user_display_name,
        workitemhistory.stage_stage_display_name,
        workitemhistory.stage_stage_type,
        workitemhistory.stage_transition_from_stage_type,        
        workitemhistory.stage_transition_to_stage_type,
        workitemhistory.stage_transition_transition_id,
        workitemhistory.stage_transition_transitioned_at,
        workitemhistory.work_item_id,
        workitemhistory.workflow_id,
        workitemhistory.stage_assigned_user_email,

    from workitemhistory, users
    where users.user_id = workitemhistory.stage_assigned_user_user_id
)
select * from fact_workitem_history
