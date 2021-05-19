{{ config(materialized='table') }}

with activities as (
    select * from "{{var('schema')}}".activities_stream
),
fact_activity as (
    select
        {{ dbt_utils.surrogate_key(['activity_id']) }} as activity_sk,
        activities.activity_id as activity_id,
        activities.context_properties_ref as work_item_reference,
        activities.context_properties_stage_type as activity_stage,
        activities.context_properties_visit_id as workitem_id,
        activities.created_by as user_id,
        activities.created_on as createdon,
        activities.data_internal_comments as activity_comments,
        activities.data_linked_work_order as work_order_reference        
    from activities
)
select * from fact_activity
