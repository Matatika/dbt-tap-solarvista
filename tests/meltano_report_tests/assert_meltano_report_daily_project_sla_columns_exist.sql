-- Assert that all these columns exist in vw_daily_project_sla
-- These are the currently specific columns used by a meltano report
-- The where causes the select to still run but return nothing
-- So if it can't select a column it still returns an error
select 
    vw_daily_project_sla.report_year,
    vw_daily_project_sla.report_month,
    vw_daily_project_sla.report_day,
    vw_daily_project_sla.day_of_month,
    vw_daily_project_sla.day_of_year,
    vw_daily_project_sla.day_of_week,
    vw_daily_project_sla.day_of_week_name,
    vw_daily_project_sla.week_key,
    vw_daily_project_sla.week_of_year,
    vw_daily_project_sla.customer_id,
    vw_daily_project_sla.customer_name,
    vw_daily_project_sla.total_open_last_7days,
    vw_daily_project_sla.total_open_last_14days,
    vw_daily_project_sla.total_open_older_than_7days,
    vw_daily_project_sla.total_open_older_than_14days,
    vw_daily_project_sla.total_rolling_open,
    vw_daily_project_sla.total_projects,
    vw_daily_project_sla.total_closed,
    vw_daily_project_sla.total_attended,
    vw_daily_project_sla.total_reactivated,
    vw_daily_project_sla.total_with_response_sla,
    vw_daily_project_sla.total_workitems,
    vw_daily_project_sla.avg_first_response_hours,
    vw_daily_project_sla.total_response_within_sla,
    vw_daily_project_sla.response_sla_percent,
    vw_daily_project_sla.avg_final_fix_hours,
    vw_daily_project_sla.total_final_fix_within_sla,
    vw_daily_project_sla.final_fix_sla_percent
from {{ ref('vw_daily_project_sla')}}
where vw_daily_project_sla.customer_id = null
