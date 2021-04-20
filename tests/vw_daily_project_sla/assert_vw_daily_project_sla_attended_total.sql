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
    , sum(total_attended)
from {{ ref('vw_daily_project_sla' ) }} vw_daily_project_sla
where report_date >= current_date - 1  -- works for all dates, but this selection reduces the test execution time
and report_date <= current_date
group by report_date, customer_id, project_type, source
having not(sum(total_attended) = (
		select
			count(distinct dim_project.reference)
		from {{ ref('fact_workitem' ) }} fact_workitem
        left join {{ ref('fact_workitem_stages' ) }} fact_workitem_stages
            using (work_item_id)
		left join {{ ref('dim_project' ) }} dim_project
			on dim_project.project_sk = fact_workitem.project_sk
		where (fact_workitem_stages.preworking_timestamp::date = vw_daily_project_sla.report_date or
				fact_workitem_stages.quickclose_timestamp::date = vw_daily_project_sla.report_date)
		and dim_project.customer_id = vw_daily_project_sla.customer_id
		and dim_project.project_type = vw_daily_project_sla.project_type
		and dim_project.source = vw_daily_project_sla.source
	)
)
