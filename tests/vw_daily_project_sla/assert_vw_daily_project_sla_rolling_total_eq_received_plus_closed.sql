-- Given a number of projects received
-- Given a number of projects closed
-- When select a customer and a day
-- Expect that current days total open equals previous day rolling total + received - closed
--
select
    report_date
	, customer_id
    , project_type
    , source
    , sum(total_open)
from {{ ref('vw_daily_project_sla' ) }} vw_daily_project_sla
where report_date >= current_date - 30  -- works for all dates, but this selection reduces the test execution time
and report_date <= current_date
group by report_date, customer_id, project_type, source
having not(sum(total_open) = sum(total_projects) + sum(total_reactivated) - sum(total_closed) + (
		select
			sum(total_projects)
		from {{ ref('vw_daily_project_sla' ) }} other
		where other.report_date = vw_daily_project_sla.report_date - 1
		and other.customer_id = vw_daily_project_sla.customer_id
		and other.project_type = vw_daily_project_sla.project_type
		and other.source = vw_daily_project_sla.source
	)
)

