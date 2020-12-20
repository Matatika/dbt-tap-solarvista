-- Given a project with multiple workitems (71602551 has 3 work items)
-- When query vw_project_sla
-- Expect total_workitems to be equal to the number of work project work items (consider using a count fact_workitem?)
select
    project_id,
    sum(total_workitems)
from {{ ref('vw_project_sla' )}}
where project_id = '71602551'
group by project_id
having not(sum(total_workitems) = 3)
union
select
    project_id,
    sum(is_closed) as total_closed
from {{ ref('vw_project_sla' )}}
where project_id = '71602551'
group by project_id
having not(sum(is_closed) = 1)