{{ config(materialized='table') }}

with workitemhistory as (
    select * from "{{var('schema')}}".workitemhistory_stream
),

fact_workitem_history as (
    select distinct 
        -- key transforms
        workitemhistory.work_item_history_id as id,

	    -- dimensions
        workitemhistory.stage_stage_type as stage,
        workitemhistory.stage_transition_received_at as received_at,
        workitemhistory.stage_transition_received_at::date as report_date,
        EXTRACT(YEAR FROM stage_transition_received_at)::integer as report_year,
        EXTRACT(MONTH FROM stage_transition_received_at)::integer as report_month,
        EXTRACT(DAY FROM stage_transition_received_at)::integer as report_day,

        -- dimension transforms
        {{ dbt_utils.surrogate_key(['stage_assigned_user_user_id']) }} as users_sk,
    
        -- metrics

        -- add everything else
    	*

    from workitemhistory
)
select * from fact_workitem_history
