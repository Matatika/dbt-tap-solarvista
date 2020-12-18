-- To check missing workitems in fact_workitem and downstream views using this fact table
-- To avoid missing sk's 
-- No rows to be returned 
select work_item_id,project_id from {{ ref('fact_workitem' )}} 
where work_item_id not in (select work_item_id from {{ ref('fact_workitem_history')}}  
where stage_stage_type = 'Closed')
and current_stage = 'Closed'
