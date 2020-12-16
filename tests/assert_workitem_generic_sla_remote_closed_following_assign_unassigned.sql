-- Given a project with multiple workitems (74125570 has 2 work items)
-- Given one of those workitems has multiple assign / unassigned (a27fc53a-eca4-45d8-9f56-6155cdbcdf83)
-- When query vw_workitem_generic_sla
-- Expect rows to be equal to 2
select
    project_id,
    count(work_item_id)
from {{ ref('vw_workitem_generic_sla' )}}
where project_id = '74125570'
group by project_id
having not(count(work_item_id) = 2)