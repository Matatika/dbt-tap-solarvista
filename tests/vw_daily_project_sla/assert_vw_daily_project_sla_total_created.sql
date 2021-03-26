-- Given a number of projects on a given day
-- When select daily project sla for that report date
-- Expect total to be equal dim_project created
select
    report_date
	, customer_id
    , project_type
    , source
    , sum(total_projects) "Total Created"
from {{ ref('vw_daily_project_sla' ) }} vw_daily_project_sla
where report_date >= current_date - 14  -- works for all dates, but this selection reduces the test execution time
and report_date <= current_date
group by report_date, customer_id, project_type, source
having not(sum(total_projects) = (
		select
			count(distinct reference)
		from {{ ref('dim_project' ) }} other
		where other.customer_id = vw_daily_project_sla.customer_id
		and other.project_type = vw_daily_project_sla.project_type
		and other.source = vw_daily_project_sla.source
		and other.createdon::date = vw_daily_project_sla.report_date
	)
)
