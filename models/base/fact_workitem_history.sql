{{ config(materialized='table') }}

with workitemhistory as (
    select * from "{{var('schema')}}".workitemhistory_stream
),

fact_workitem_history as (
    select distinct 
        -- key transforms
        workitemhistory.work_item_history_id as id,

        -- dimension transforms
        {{ dbt_utils.surrogate_key(['stage_assigned_user_user_id']) }} as users_sk,
        stage_stage_type as stage,
    
        -- metrics

        -- add everything else
    	*

    from workitemhistory
)
select * from fact_workitem_history
