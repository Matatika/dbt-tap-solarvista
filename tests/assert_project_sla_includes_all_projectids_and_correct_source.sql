-- Assert that the source columns is now in the vw_project_sla table
-- Check if the source column has null values
-- Check if any project_ids and source exist in fact_workitem and not in vw_project_sla

select 
    vw_project_sla.project_id,
	vw_project_sla.source
from {{ ref('vw_project_sla')}}
where vw_project_sla.source = null
union
select 
	vw_project_sla.project_id,
	vw_project_sla.source
from {{ ref('vw_project_sla')}}
left join {{ ref('fact_workitem')}} on fact_workitem.project_id = vw_project_sla.project_id
where fact_workitem.project_id is null
