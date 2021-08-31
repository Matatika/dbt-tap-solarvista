-- Given a number of projects received
-- Given a number of projects closed
-- When select a customer and a day
-- Expect that current days total open equals previous day rolling total + received - closed

select
    report_date
	, customer_id
    , project_type
    , sum(total_open) "Total Open Today"
from {{ ref('vw_daily_projects' ) }} vw_daily_projects
where report_date >= current_date - 1  -- works for all dates, but this selection reduces the test execution time
and report_date <= current_date
group by report_date, customer_id, project_type
having not(sum(total_open) = (
		select
			count(distinct reference)
		from {{ ref('dim_project_snapshot' ) }} other
		where other.customer_id = vw_daily_projects.customer_id
		and other.project_type = vw_daily_projects.project_type
		and other.status = 'Active'
		and dbt_valid_from::date <= vw_daily_projects.report_date 
		and (dbt_valid_to::date > vw_daily_projects.report_date or dbt_valid_to is null)
	)
)
