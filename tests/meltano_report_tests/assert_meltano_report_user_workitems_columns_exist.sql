-- Assert that all these columns exist in vw_user_workitems
-- These are the currently specific columns used by a meltano report
-- The where causes the select to still run but return nothing
-- So if it can't select a column it still returns an error
select
    vw_user_workitems.work_item_id,
    vw_user_workitems.report_year,
    vw_user_workitems.report_month,
    vw_user_workitems.report_day,
    vw_user_workitems.day_of_month,
    vw_user_workitems.day_of_year,
    vw_user_workitems.day_of_week,
    vw_user_workitems.day_of_week_name,
    vw_user_workitems.assigned_user_id,
    vw_user_workitems.assigned_user_name,
    vw_user_workitems.attended_user_id,
    vw_user_workitems.attended_user_name,
    vw_user_workitems.remoteclosed_user_id,
    vw_user_workitems.remoteclosed_user_name,
    vw_user_workitems.territory_id,
    vw_user_workitems.territory_name,
    vw_user_workitems.site_id,
    vw_user_workitems.site_name,
    vw_user_workitems.customer_id,
    vw_user_workitems.customer_name,
    vw_user_workitems.duration_hours,
    vw_user_workitems.workitem_count
from {{ ref('vw_user_workitems')}}
where vw_user_workitems.work_item_id = null
