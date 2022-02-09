-- Assert that there are equal numbers of total jobs in both vw_project_sla and fact_workitem.

select 
    sum(vw_project_sla.total_workitems) - (select count(fact_workitem.work_item_id) from {{ ref('fact_workitem')}} where fact_workitem.project_id is not null) as difference
from {{ ref('vw_project_sla')}}
having not sum(vw_project_sla.total_workitems) - (select count(fact_workitem.work_item_id) from {{ ref('fact_workitem')}}
                                                    left join {{ ref('dim_project')}} on fact_workitem.project_sk = dim_project.project_sk
                                                    where fact_workitem.project_id is not null
                                                    and dim_project.closedon is not null) = 0
