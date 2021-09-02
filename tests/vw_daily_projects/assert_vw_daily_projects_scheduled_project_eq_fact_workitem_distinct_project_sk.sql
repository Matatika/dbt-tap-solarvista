-- Expect that current days total scheduled is eq to total PROJECTS 
-- with a work item scheduled on current day
--
select
    report_date
	, customer_id
    , project_type
    , sum(total_scheduled)
from {{ ref('vw_daily_projects' ) }} vw_daily_projects
where report_date >= current_date - 1  -- works for all dates, but this selection reduces the test execution time
and report_date <= current_date
group by report_date, customer_id, project_type
having not(sum(total_scheduled) = (
		select
			count(distinct fact_workitem.project_sk)
		from {{ ref('fact_workitem' ) }} fact_workitem
		left join {{ ref('dim_project' ) }} dim_project
			on dim_project.project_sk = fact_workitem.project_sk
		where dim_project.customer_id = vw_daily_projects.customer_id
		and dim_project.project_type = vw_daily_projects.project_type
		and fact_workitem.schedule_start_date = vw_daily_projects.report_date
	)
)
