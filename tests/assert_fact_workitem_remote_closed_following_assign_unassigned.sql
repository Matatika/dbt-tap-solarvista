-- Given a project with multiple workitems (74125570 has 2 work items)
-- Given one of those workitems has multiple assign / unassigned (a27fc53a-eca4-45d8-9f56-6155cdbcdf83)
-- When query fact_workitem
-- Expect rows to be equal to 2

select
    project_id,
    count(work_item_id)
from {{ ref('fact_workitem' )}}
where project_id = '74125570'
group by project_id
having not(count(work_item_id) = 2)
union
select
    project_id,
    sum(is_closed) as total_closed
from {{ ref('vw_project_sla' )}}
where project_id = '74125570'
group by project_id
having not(sum(is_closed) = 1)
