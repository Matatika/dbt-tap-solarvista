-- Expect that current days total attended is eq to total PROJECTS attended from fact_workitem
--
select
    report_date
    , sum(total_attended)
from {{ ref('vw_daily_projects' ) }} vw_daily_projects
where report_date >= current_date - 5  -- works for all dates, but this selection reduces the test execution time
and report_date <= current_date
group by report_date
having not(sum(total_attended) = (
		select
			count(distinct fact_workitem.project_sk)
		from {{ ref('fact_workitem' ) }} fact_workitem
        left join {{ ref('fact_workitem_stages' ) }} fact_workitem_stages
            on fact_workitem_stages.work_item_id = fact_workitem.work_item_id
		left join {{ ref('dim_project' ) }} dim_project
			on dim_project.project_sk = fact_workitem.project_sk
		where (fact_workitem_stages.preworking_timestamp::date = vw_daily_projects.report_date or
				fact_workitem_stages.quickclose_timestamp::date = vw_daily_projects.report_date)
		and fact_workitem.project_sk notnull
		and fact_workitem.schedule_start_date = vw_daily_projects.report_date
	)
)
