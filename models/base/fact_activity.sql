{{ config(materialized='table') }}

with activity as (
    select * from {{ source ('solarvista_source', 'activity_stream') }}
),
fact_activity as (
    select
        {{ dbt_utils.surrogate_key(['activity_id']) }} as activity_sk,
        activity.activity_id as activity_id,
        activity.context_properties_ref as work_item_reference,
        activity.context_properties_stage_type as activity_stage,
        activity.context_properties_visit_id as work_item_id,
        activity.created_by as user_id,
        activity.created_on as createdon,
        activity.data_internal_comments as activity_comments,
        activity.data_linked_work_order as linked_work_order_reference        
    from activity
)
select * from fact_activity
