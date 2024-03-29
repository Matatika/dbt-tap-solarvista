-- Given a number of projects, for a customer, of a project_type
-- Given those projects were created on a given date
-- Given a number of work items for those projects
-- When select daily project sla for that report date
-- Expect total workitems to be equal for fact_workitem for the same customer and project criteria
select
    report_date
	, customer_id
    , project_type
    , sum(total_workitems)
from {{ ref('vw_daily_projects' ) }} vw_daily_projects
where report_date >= current_date - 1  -- works for all dates, but this selection reduces the test execution time
and report_date <= current_date
group by report_date, customer_id, project_type
having not(sum(total_workitems) = (
		select
			count(distinct fact_workitem.work_item_id)
		from {{ ref('fact_workitem' ) }} fact_workitem
		left join {{ ref('dim_project' ) }} dim_project
			on dim_project.project_sk = fact_workitem.project_sk
		where fact_workitem.created_on::date = vw_daily_projects.report_date
		and dim_project.customer_id = vw_daily_projects.customer_id
		and dim_project.project_type = vw_daily_projects.project_type
	)
)
