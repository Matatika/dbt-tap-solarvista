-- Assert that there are equal numbers of total jobs in both vw_project_sla and fact_workitem.

select 
    sum(vw_project_sla.total_workitems) - (select count(fact_workitem.work_item_id) from {{ ref('fact_workitem')}} where fact_workitem.project_id notnull) as difference
from {{ ref('vw_project_sla')}}
having not sum(vw_project_sla.total_workitems) - (select count(fact_workitem.work_item_id) from {{ ref('fact_workitem')}} where fact_workitem.project_id notnull) = 0
