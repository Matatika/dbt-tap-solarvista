-- Given a number of projects
-- Given a number of work items
-- When select a customer and a day
-- Expect that current days total attended is eq to total PROJECTS attended from fact_workitem_history
--
select
    report_date
	, customer_id
    , project_type
    , source
    , sum(total_scheduled)
from {{ ref('vw_daily_projects' ) }} vw_daily_projects
where report_date >= current_date - 1  -- works for all dates, but this selection reduces the test execution time
and report_date <= current_date
group by report_date, customer_id, project_type, source
having not(sum(total_scheduled) = (
		select
			count(distinct fact_workitem.project_sk)
		from {{ ref('fact_workitem' ) }} fact_workitem
		left join {{ ref('dim_project' ) }} dim_project
			on dim_project.project_sk = fact_workitem.project_sk
		where dim_project.customer_id = vw_daily_projects.customer_id
		and dim_project.project_type = vw_daily_projects.project_type
		and dim_project.source = vw_daily_projects.source
		and fact_workitem.schedule_start_date = vw_daily_projects.report_date
	)
)
