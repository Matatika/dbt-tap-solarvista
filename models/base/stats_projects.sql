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
        min(projects.appliedresponsesla) as appliedresponsesla,
        min(projects.responseduedate) as responseduedate,
        min(projects.fixduedate) as fixduedate,

        -- aggregations
        count(workitem_facts.work_item_id) as number_workitems,
        min(workitem_stages.assigned) as first_response,
        {{ dbt_utils.datediff('projects.createdon', 'min(workitem_stages.assigned)', 'hour') }} as first_response_hours,
        max(workitem_stages.closed) as final_fix,
        {{ dbt_utils.datediff('projects.createdon', 'max(workitem_stages.closed)', 'hour') }} as final_fix_hours

    from workitem_facts, workitem_stages, projects
    where workitem_stages.work_item_id = workitem_facts.work_item_id
    and projects.reference = workitem_facts.project_id
    group by project_id, createdon
)

select
	project_id,
	createdon,
	appliedresponsesla,
	number_workitems,
    first_response,
	first_response_hours,
    final_fix,
	final_fix_hours,

	-- aggregations
	(case when {{ dbt_utils.datediff('first_response', 'responseduedate', 'hour') }} <= 0 then 1 else 0 end) as response_within_sla,
	(case when {{ dbt_utils.datediff('final_fix', 'fixduedate', 'hour') }} <= 0 then 1 else 0 end) as final_fix_within_sla

from project_stats
