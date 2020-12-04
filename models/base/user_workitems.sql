{{ config(materialized='table') }}

with workitem_facts as (
    select
        assigned_user_id,
        min(created_on) as first_workitem_date,
        max(created_on) as most_recent_workitem_date,
        sum(duration_hours) as total_duration_hours,
        sum(workitem_count) as total_workitems
    from {{ ref('fact_workitem') }}
    group by assigned_user_id
),

users as (
    select * from {{ ref('dim_user') }}
),

user_workitems as (
    select
        users.user_id,
        users.display_name,
        workitem_facts.first_workitem_date,
        workitem_facts.most_recent_workitem_date,
        workitem_facts.total_duration_hours,
        coalesce(workitem_facts.total_workitems, 0) as total_workitems
    from users
    left join workitem_facts on users.user_id = workitem_facts.assigned_user_id
)
select * from user_workitems