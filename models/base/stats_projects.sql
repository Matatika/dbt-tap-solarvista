{{ config(materialized='table') }}

with workitem_facts as (
     select * from {{ ref('fact_workitem') }}
),

workitem_stages as (
     select * from {{ ref('dim_workitem_stages') }}
),

projects as (
     select * from {{ ref('dim_project') }}
),

project_stats as (
    select
        project_id,
        projects.createdon,
        projects.appliedresponsesla,

        -- aggregations
        count(workitem_facts.work_item_id) as number_workitems,
        min(workitem_stages.assigned) as first_response,
        {{ dbt_utils.datediff('projects.createdon', 'min(workitem_stages.assigned)', 'hour') }} as first_response_hours,
        (case when {{ dbt_utils.datediff('projects.createdon', 'min(workitem_stages.assigned)', 'hour') }} < appliedresponsesla then 1 else 0 end) as response_within_sla

    from workitem_facts
        left join workitem_stages on workitem_stages.work_item_id = workitem_facts.work_item_id
        left join projects on projects.reference = workitem_facts.project_id
    group by project_id, createdon, appliedresponsesla
)
select * from project_stats
