{{ config(materialized='table') }}

with workitem_facts as (
     select * from {{ ref('fact_workitem') }}
),

workitem_stages as (
     select * from {{ ref('vw_workitem_stages') }}
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
        min(workitem_stages.accepted_timestamp) as accepted_timestamp,
        min(workitem_stages.closed_timestamp) as closed_timestamp,  
        min(workitem_stages.assigned_timestamp) as assigned_timestamp,  
        min(workitem_stages.cancelled_timestamp) as cancelled_timestamp,  
        min(workitem_stages.discarded_timestamp) as discarded_timestamp,  
        min(workitem_stages.postworking_timestamp) as postworking_timestamp,  
        min(workitem_stages.preworking_timestamp) as preworking_timestamp,  
        min(workitem_stages.quickclose_timestamp) as quickclose_timestamp,  
        min(workitem_stages.remoteclosed_timestamp) as remoteclosed_timestamp,  
        min(workitem_stages.travellingfrom_timestamp) as travellingfrom_timestamp,  
        min(workitem_stages.travellingto_timestamp) as travellingto_timestamp,  
        min(workitem_stages.unassigned_timestamp) as unassigned_timestamp,  
        min(workitem_stages.working_timestamp) as working_timestamp,
	    (case 
		     when min(projects.status) = 'Cancelled' then 0 
		     when min(projects.status) = 'Closed' then 0 
		     when min(workitem_stages.closed_timestamp) is null then 1 
		 end) as is_open,
        (case 
		     when min(projects.status) = 'Closed' then 1 
		     when min(workitem_stages.closed_timestamp) is not null then 1 
		 end) as is_closed,
	    (case 
		     when min(projects.status) = 'Cancelled' then 1 
		     when min(workitem_stages.cancelled_timestamp) is null then 1 
		 end) as is_cancelled,
        min(workitem_stages.preworking_timestamp) as first_response,
        {{ dbt_utils.datediff('min(projects.createdon)', 'min(workitem_stages.preworking_timestamp)', 'hour') }} as first_response_hours,
        max(workitem_stages.closed_timestamp) as final_fix,
        {{ dbt_utils.datediff('min(projects.createdon)', 'max(workitem_stages.closed_timestamp)', 'hour') }} as final_fix_hours

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
        min(accepted_timestamp) as accepted_timestamp,
        min(closed_timestamp) as closed_timestamp,  
        min(assigned_timestamp) as assigned_timestamp,  
        min(cancelled_timestamp) as cancelled_timestamp,  
        min(discarded_timestamp) as discarded_timestamp,  
        min(postworking_timestamp) as postworking_timestamp,  
        min(preworking_timestamp) as preworking_timestamp,  
        min(quickclose_timestamp) as quickclose_timestamp,  
        min(remoteclosed_timestamp) as remoteclosed_timestamp,  
        min(travellingfrom_timestamp) as travellingfrom_timestamp,  
        min(travellingto_timestamp) as travellingto_timestamp,  
        min(unassigned_timestamp) as unassigned_timestamp,  
        min(working_timestamp) as working_timestamp,
		min(first_response) as first_response,
		min(first_response_hours) as first_response_hours,
		min(final_fix) as final_fix,
		min(final_fix_hours) as final_fix_hours,

		-- aggregations
        count(project_id) as total_projects,
		sum(number_workitems) as total_workitems,
		sum(is_open) as total_open,
		sum(is_closed) as total_closed,
		(case when min(cancelled_timestamp) is not null then 1 when {{ dbt_utils.datediff('min(responsedue_date)', 'min(first_response)', 'hour') }} <= 0 then 1 else 0 end) as response_within_sla,
		(case when min(cancelled_timestamp) is not null then 1 when {{ dbt_utils.datediff('min(fixdue_date)', 'min(final_fix)', 'hour') }} <= 0 then 1 else 0 end) as final_fix_within_sla

    from project_stats
    group by project_id, report_date, report_year, report_month, report_day
    order by report_year ASC, report_month ASC, report_day ASC
)				
select * from stats
