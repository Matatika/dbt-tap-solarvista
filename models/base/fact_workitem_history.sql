{{
    config(materialized='incremental'),
    unique_key='work_item_history_id'
}}

with workitemhistory as (
    select * from "{{var('schema')}}".workitemhistory_stream
),
to_users as (
    select * from {{ ref('dim_user') }}
),
by_users as (
    select * from {{ ref('dim_user') }}
),
fact_workitem_history as (
    select distinct 
        -- key transforms
        workitemhistory.work_item_history_id as id

        --SCD surrogate keys for join purposes in reporting layer
        ,to_users.users_sk as to_users_sk
        ,by_users.users_sk as by_users_sk

	    -- dimensions
        ,workitemhistory.last_modified
        ,workitemhistory.stage_transition_received_at::date as report_date
        ,EXTRACT(YEAR FROM stage_transition_received_at)::integer as report_year
        ,EXTRACT(MONTH FROM stage_transition_received_at)::integer as report_month
        ,EXTRACT(DAY FROM stage_transition_received_at)::integer as report_day
        ,to_users.user_id as assigned_to_user_id
        ,to_users.display_name as assigned_to_user_name
        ,by_users.user_id as assigned_by_user_id
        ,by_users.display_name as assigned_by_user_name

        -- metrics

        --all stage transition columns from workitem history stage table
        ,workitemhistory.stage_assigned_user_display_name
        ,workitemhistory.stage_assigned_user_user_id
        ,workitemhistory.stage_stage_display_name
        ,workitemhistory.stage_stage_type
        ,workitemhistory.stage_transition_from_stage_type
        ,workitemhistory.stage_transition_received_at
        ,workitemhistory.stage_transition_to_stage_type
        ,workitemhistory.stage_transition_transition_id
        ,workitemhistory.stage_transition_transitioned_at
        ,workitemhistory.stage_transition_transitioned_by_display_name
        ,workitemhistory.stage_transition_transitioned_by_user_id
        ,workitemhistory.work_item_history_id
        ,workitemhistory.work_item_id
        ,workitemhistory.workflow_id
        ,workitemhistory.stage_assigned_user_email

    from workitemhistory
	left join to_users on to_users.user_id = workitemhistory.stage_assigned_user_user_id
	left join by_users on by_users.user_id = workitemhistory.stage_transition_transitioned_by_user_id
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where last_modified > (select max(t2.last_modified) from {{ this }} as t2)
{% endif %}
)
select * from fact_workitem_history
