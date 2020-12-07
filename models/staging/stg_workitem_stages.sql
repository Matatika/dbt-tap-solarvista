{{ config(materialized='table') }}

with stg_workitem_stages as (
    select distinct
        work_item_id, 
        stage_stage_type as stage,
        min(stage_transition_received_at) as first_received_at,
        max(stage_transition_received_at) as last_received_at
    from {{ ref('fact_workitem_history') }}
    group by work_item_id, stage_stage_type
    order by work_item_id
)
select * from stg_workitem_stages
