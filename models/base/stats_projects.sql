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
        projects.reference as project_id,
	    min(projects.project_type) as type,
	    min(projects.status) as status,
        min(projects.createdon) as createdon,
        min(projects.appliedresponsesla) as appliedresponsesla,
        min(projects.responseduedate) as responsedue_date,
        min(projects.fixduedate) as fixdue_date,

        -- aggregations
        sum(workitem_count) as number_workitems,
	    (case 
		     when min(projects.status) = 'Cancelled' then 0 
		     when min(projects.status) = 'Closed' then 0 
		     when min(workitem_stages.closed) is null then 1 
		 end) as is_open,
        min(workitem_stages.closed) as closed_date,
        (case 
		     when min(projects.status) = 'Closed' then 1 
		     when min(workitem_stages.closed) is not null then 1 
		 end) as is_closed,
        min(workitem_stages.cancelled) as cancelled_date,
	    (case 
		     when min(projects.status) = 'Cancelled' then 1 
		     when min(workitem_stages.cancelled) is null then 1 
		 end) as is_cancelled,
        min(workitem_stages.assigned) as first_response,
        {{ dbt_utils.datediff('min(projects.createdon)', 'min(workitem_stages.assigned)', 'hour') }} as first_response_hours,
        max(workitem_stages.closed) as final_fix,
        {{ dbt_utils.datediff('min(projects.createdon)', 'max(workitem_stages.closed)', 'hour') }} as final_fix_hours

    from projects
	left join workitem_facts on workitem_facts.project_id = projects.reference
	left join workitem_stages on workitem_stages.work_item_id = workitem_facts.work_item_id
    group by projects.reference
),

stats as (
    select distinct
        project_id,
        createdon::date as report_date,
        EXTRACT(YEAR FROM createdon)::integer as report_year,
        EXTRACT(MONTH FROM createdon)::integer as report_month,
        EXTRACT(DAY FROM createdon)::integer as report_day,

	    min(type) as type,
	    min(status) as status,
        min(createdon) as createdon,
        min(appliedresponsesla) as appliedresponsesla,
        min(responsedue_date) as responsedue_date,
        min(fixdue_date) as fixdue_date,
		min(cancelled_date) as cancelled_date,
		min(closed_date) as closed_date,
		min(first_response) as first_response,
		min(first_response_hours) as first_response_hours,
		min(final_fix) as final_fix,
		min(final_fix_hours) as final_fix_hours,

		-- aggregations
        count(project_id) as total_projects,
		sum(number_workitems) as total_workitems,
		sum(is_open) as total_open,
		sum(is_closed) as total_closed,
		(case when min(cancelled_date) is not null then 1 when {{ dbt_utils.datediff('min(responsedue_date)', 'min(first_response)', 'hour') }} <= 0 then 1 else 0 end) as response_within_sla,
		(case when min(cancelled_date) is not null then 1 when {{ dbt_utils.datediff('min(fixdue_date)', 'min(final_fix)', 'hour') }} <= 0 then 1 else 0 end) as final_fix_within_sla

    from project_stats
    group by project_id, report_date, report_year, report_month, report_day
    order by report_year ASC, report_month ASC, report_day ASC
)				
select * from stats
