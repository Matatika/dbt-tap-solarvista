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
    select distinct
        project_id,
        min(projects.createdon) as createdon,
        min(projects.appliedresponsesla) as appliedresponsesla,
        min(projects.responseduedate) as responseduedate,
        min(projects.fixduedate) as fixduedate,

        -- aggregations
        count(workitem_facts.work_item_id) as number_workitems,
        min(workitem_stages.cancelled) as cancelled,
        min(workitem_stages.assigned) as first_response,
        {{ dbt_utils.datediff('min(projects.createdon)', 'min(workitem_stages.assigned)', 'hour') }} as first_response_hours,
        max(workitem_stages.closed) as final_fix,
        {{ dbt_utils.datediff('min(projects.createdon)', 'max(workitem_stages.closed)', 'hour') }} as final_fix_hours

    from workitem_facts, workitem_stages, projects
    where workitem_stages.work_item_id = workitem_facts.work_item_id
    and projects.reference = workitem_facts.project_id
    group by project_id
),

stats as (
    select distinct
        project_id,
        createdon::date as report_date,
        EXTRACT(YEAR FROM createdon)::integer as report_year,
        EXTRACT(MONTH FROM createdon)::integer as report_month,
        EXTRACT(DAY FROM createdon)::integer as report_day,

        min(createdon) as createdon,
        min(appliedresponsesla) as appliedresponsesla,
        min(responseduedate) as responseduedate,
        min(fixduedate) as fixduedate,
		min(cancelled) as cancelled,
		min(first_response) as first_response,
		min(first_response_hours) as first_response_hours,
		min(final_fix) as final_fix,
		min(final_fix_hours) as final_fix_hours,

		-- aggregations
        count(project_id) as total_projects,
		sum(number_workitems) as total_workitems,
		(case when min(cancelled) is not null then 1 when {{ dbt_utils.datediff('min(responseduedate)', 'min(first_response)', 'hour') }} <= 0 then 1 else 0 end) as response_within_sla,
		(case when min(cancelled) is not null then 1 when {{ dbt_utils.datediff('min(fixduedate)', 'min(final_fix)', 'hour') }} <= 0 then 1 else 0 end) as final_fix_within_sla

    from project_stats
    group by project_id, report_date, report_year, report_month, report_day
    order by report_year ASC, report_month ASC, report_day ASC
)				
select * from stats
