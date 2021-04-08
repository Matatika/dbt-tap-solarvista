-- Assert that there are equal numbers of total jobs in both vw_project_sla and fact_workitem.
-- We now only calculate SLA when closed, so must have a project with closedon date

select 
    sum(vw_project_sla.total_workitems) - (select count(fact_workitem.work_item_id) from {{ ref('fact_workitem')}} where fact_workitem.project_id notnull) as difference
from {{ ref('vw_project_sla')}}
having not sum(vw_project_sla.total_workitems) - (
        select count(fact_workitem.work_item_id) 
        from {{ ref('fact_workitem')}} 
        left join {{ ref('dim_project')}} on dim_project.reference = fact_workitem.project_id
        where dim_project.closedon is not null
    ) = 0
