-- Assert that all these columns exist in vw_daily_projects
-- These are the currently specific columns used by a meltano report
-- The where causes the select to still run but return nothing
-- So if it can't select a column it still returns an error
select 
    vw_daily_projects.report_year,
    vw_daily_projects.report_month,
    vw_daily_projects.report_day,
    vw_daily_projects.day_of_month,
    vw_daily_projects.day_of_year,
    vw_daily_projects.day_of_week,
    vw_daily_projects.day_of_week_name,
    vw_daily_projects.week_key,
    vw_daily_projects.week_of_year,
    vw_daily_projects.customer_id,
    vw_daily_projects.customer_name,
    vw_daily_projects.total_open_last_7days,
    vw_daily_projects.total_open_last_14days,
    vw_daily_projects.total_open_older_than_7days,
    vw_daily_projects.total_open_older_than_14days,
    vw_daily_projects.total_rolling_open,
    vw_daily_projects.total_projects,
    vw_daily_projects.total_closed,
    vw_daily_projects.total_scheduled,
    vw_daily_projects.total_attended,
    vw_daily_projects.total_reactivated,
    vw_daily_projects.total_with_response_sla,
    vw_daily_projects.total_workitems,
    vw_daily_projects.avg_first_response_hours,
    vw_daily_projects.total_response_within_sla,
    vw_daily_projects.response_sla_percent,
    vw_daily_projects.avg_final_fix_hours,
    vw_daily_projects.total_final_fix_within_sla,
    vw_daily_projects.final_fix_sla_percent
from {{ ref('vw_daily_projects')}}
where vw_daily_projects.customer_id = null
