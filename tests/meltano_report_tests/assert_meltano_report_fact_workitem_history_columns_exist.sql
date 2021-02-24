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