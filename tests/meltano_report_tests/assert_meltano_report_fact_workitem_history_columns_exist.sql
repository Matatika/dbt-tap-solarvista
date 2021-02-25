-- Assert that all these columns exist in fact_workitem_history
-- These are the currently specific columns used by a meltano report
-- The where causes the select to still run but return nothing
-- So if it can't select a column it still returns an error
select
    fact_workitem_history.report_year,
    fact_workitem_history.report_month,
    fact_workitem_history.report_day,
    fact_workitem_history.work_item_history_id,
    fact_workitem_history.work_item_id,
    fact_workitem_history.stage_stage_type,
    fact_workitem_history.stage_stage_display_name,
    fact_workitem_history.assigned_to_user_id,
    fact_workitem_history.assigned_to_user_name,
    fact_workitem_history.assigned_by_user_id,
    fact_workitem_history.assigned_by_user_name
from {{ ref('fact_workitem_history')}}
where fact_workitem_history.work_item_id = null