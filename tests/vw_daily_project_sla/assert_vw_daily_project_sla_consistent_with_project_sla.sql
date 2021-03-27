-- Given a number of daily summarised project results
-- Expect consistency with % calculated based on vw_project_sla
select
	report_date, customer_id, min(response_sla_percent), min(final_fix_sla_percent), sum(total_with_final_fix_sla), sum(total_final_fix_within_sla)
from {{ ref('vw_daily_project_sla' ) }} vw_daily_project_sla
where date_day >= current_date - 14
and date_day < current_date - 1
group by report_date, customer_id, project_type, source
having not (min(response_sla_percent) = (
		SELECT 
			round( ((sum("project_sla"."response_within_sla") / NULLIF(sum("project_sla"."total_projects"), 0)) * 100)::numeric, 2) "project_sla.in_response_sla_pc"
		FROM {{ ref('vw_project_sla' ) }} as project_sla 
		WHERE project_sla.customer_id = vw_daily_project_sla.customer_id
		AND project_sla.report_date = vw_daily_project_sla.report_date
		AND project_sla.project_type = vw_daily_project_sla.project_type
		AND project_sla.source = vw_daily_project_sla.source
		AND NOT project_sla.appliedresponsesla IS NULL
		AND NOT project_sla.responseduedate IS NULL 
		GROUP BY project_sla.report_date
	)
)
union
select
	report_date, customer_id, min(response_sla_percent), min(final_fix_sla_percent), sum(total_with_final_fix_sla), sum(total_final_fix_within_sla)
from {{ ref('vw_daily_project_sla' ) }} vw_daily_project_sla
where date_day >= current_date - 14
and date_day < current_date - 1
group by report_date, customer_id, project_type, source
having not (min(final_fix_sla_percent) = (
		SELECT 
			round( ((sum("project_sla"."final_fix_within_sla") / NULLIF(sum("project_sla"."total_projects"), 0)) * 100)::numeric, 2) "project_sla.in_fix_sla_pc"
		FROM {{ ref('vw_project_sla' ) }} as project_sla 
		WHERE project_sla.customer_id = vw_daily_project_sla.customer_id
		AND project_sla.report_date = vw_daily_project_sla.report_date
		AND project_sla.project_type = vw_daily_project_sla.project_type
		AND project_sla.source = vw_daily_project_sla.source
		AND NOT project_sla.appliedfixsla IS NULL
		AND NOT project_sla.fixduedate IS NULL 
		GROUP BY project_sla.report_date
	)
)
union
select
	report_date, customer_id, min(response_sla_percent), min(final_fix_sla_percent), sum(total_with_final_fix_sla), sum(total_final_fix_within_sla)
from {{ ref('vw_daily_project_sla' ) }} vw_daily_project_sla
where date_day >= current_date - 14
and date_day < current_date - 1
group by report_date, customer_id, project_type, source
having not (sum(total_with_final_fix_sla) = (
		SELECT 
			NULLIF(sum("project_sla"."total_projects"), 0) "project_sla.total_with_final_fix_sla"
		FROM {{ ref('vw_project_sla' ) }} as project_sla 
		WHERE project_sla.customer_id = vw_daily_project_sla.customer_id
		AND project_sla.report_date = vw_daily_project_sla.report_date
		AND project_sla.project_type = vw_daily_project_sla.project_type
		AND project_sla.source = vw_daily_project_sla.source
		AND NOT project_sla.appliedfixsla IS NULL
		AND NOT project_sla.fixduedate IS NULL 
		GROUP BY project_sla.report_date
	)
)
union
select
	report_date, customer_id, min(response_sla_percent), min(final_fix_sla_percent), sum(total_with_final_fix_sla), sum(total_final_fix_within_sla)
from {{ ref('vw_daily_project_sla' ) }} vw_daily_project_sla
where date_day >= current_date - 14
and date_day < current_date - 1
group by report_date, customer_id, project_type, source
having not (sum(total_final_fix_within_sla) = (
		SELECT 
			sum("project_sla"."final_fix_within_sla") "project_sla.total_final_fix_within_sla"
		FROM {{ ref('vw_project_sla' ) }} as project_sla 
		WHERE project_sla.customer_id = vw_daily_project_sla.customer_id
		AND project_sla.report_date = vw_daily_project_sla.report_date
		AND project_sla.project_type = vw_daily_project_sla.project_type
		AND project_sla.source = vw_daily_project_sla.source
		AND NOT project_sla.appliedfixsla IS NULL
		AND NOT project_sla.fixduedate IS NULL 
		GROUP BY project_sla.report_date
	)
)
