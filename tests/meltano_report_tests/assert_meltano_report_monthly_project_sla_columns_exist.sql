-- Assert that all these columns exist in fact_workitem_history
-- These are the currently specific columns used by a meltano report
-- The where causes the select to still run but return nothing
-- So if it can't select a column it still returns an error
select
    vw_monthly_project_sla.report_year,
    vw_monthly_project_sla.report_month,
    vw_monthly_project_sla.month_start,
    vw_monthly_project_sla.month_start,
    vw_monthly_project_sla.total_open,
    vw_monthly_project_sla.total_projects,
    vw_monthly_project_sla.total_closed,
    vw_monthly_project_sla.total_workitems,
    vw_monthly_project_sla.avg_first_response_hours,
    vw_monthly_project_sla.total_response_within_sla,
    vw_monthly_project_sla.response_sla_percent,
    vw_monthly_project_sla.first_fix_sla_percent,
    vw_monthly_project_sla.avg_final_fix_hours,
    vw_monthly_project_sla.total_final_fix_within_sla,
    vw_monthly_project_sla.final_fix_sla_percent
from {{ ref('vw_monthly_project_sla')}}
where vw_monthly_project_sla.total_projects = null
