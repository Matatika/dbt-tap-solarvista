--To check workitems status match between current and history workitems fact table
--Test workitem id - 01accd16-8ac2-48a6-abe5-68af028f2c9b
--No rows in history when workitem is in unassigned status in fact table 
select f1.work_item_id,fwh.stage_stage_type 
from {{ ref('fact_workitem' )}} f1, {{ ref('fact_workitem_history')}} fwh
where f1.current_stage = 'Unassigned'
and fwh.stage_stage_type = 'Closed'
and f1.work_item_id = fwh.work_item_id 
--and f1.work_item_id = '01accd16-8ac2-48a6-abe5-68af028f2c9b'