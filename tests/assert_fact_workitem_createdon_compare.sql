--Check if workitem creation and history stage transition time are same 
--No rows returned as of now (to validate if this is expected functionality and get details on createdon time for history rows)
select * from {{ ref('fact_workitem' )}} f1, {{ ref('fact_workitem_history')}} fwh
where f1.created_on = fwh.stage_transition_received_at 
and f1.work_item_id = fwh.work_item_id 