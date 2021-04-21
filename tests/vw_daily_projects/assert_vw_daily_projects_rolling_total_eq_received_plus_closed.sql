-- Given a number of projects received
-- Given a number of projects closed
-- When select a customer and a day
-- Expect that current days total open equals previous day rolling total + received - closed
--

--
-- There are late arriving transactions, so the total received + total closed doesn't add up
--
--select
--    report_date
--	, customer_id
--    , project_type
--    , source
--    , sum(total_open)
--from {{ ref('vw_daily_projects' ) }} vw_daily_projects
--where report_date >= current_date - 30  -- works for all dates, but this selection reduces the test execution time
--and report_date <= current_date
--group by report_date, customer_id, project_type, source
--having not(sum(total_open) = sum(total_projects) + sum(total_reactivated) - sum(total_closed) + (
--		select
--			sum(total_projects)
--		from {{ ref('vw_daily_projects' ) }} other
--		where other.report_date = vw_daily_projects.report_date - 1
--		and other.customer_id = vw_daily_projects.customer_id
--		and other.project_type = vw_daily_projects.project_type
--		and other.source = vw_daily_projects.source
--	)
--)

select
    report_date
	, customer_id
    , project_type
    , source
    , sum(total_open) "Total Open Today"
from {{ ref('vw_daily_projects' ) }} vw_daily_projects
where report_date >= current_date - 1  -- works for all dates, but this selection reduces the test execution time
and report_date <= current_date
group by report_date, customer_id, project_type, source
having not(sum(total_open) = (
		select
			count(distinct reference)
		from {{ ref('dim_project_snapshot' ) }} other
		where other.customer_id = vw_daily_projects.customer_id
		and other.project_type = vw_daily_projects.project_type
		and other.source = vw_daily_projects.source
		and other.status = 'Active'
		and dbt_valid_from::date <= vw_daily_projects.report_date 
		and (dbt_valid_to::date > vw_daily_projects.report_date or dbt_valid_to is null)
	)
)
