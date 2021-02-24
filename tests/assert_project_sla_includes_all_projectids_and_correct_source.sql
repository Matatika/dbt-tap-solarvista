-- Assert that the source column is now in the vw_project_sla table
-- Check if any project_ids and source exist just in fact_workitem or vw_project_sla

select 
	vw_project_sla.project_id,
	vw_project_sla.source
from {{ ref('vw_project_sla')}}
left join {{ ref('fact_workitem')}} on fact_workitem.project_id = vw_project_sla.project_id
where fact_workitem.project_id is null or vw_project_sla.project_id is null
