-- Given a project with a 'Reactivation' tagged work item
-- When query vw_project_sla
-- Expect reactivation_timestamp
-- Expect is_refix to be equal to 1 (returning results is a test failure)
select
    project_id,
    is_refix
from {{ ref('vw_project_sla' )}}
where is_refix is null
union
select
    project_id,
    is_refix
from {{ ref('vw_project_sla' )}}
where project_id = '74864898'
and is_refix = 0
union
select
    project_id,
    is_refix
from {{ ref('vw_project_sla' )}} vp
where reactivated_timestamp is not null
and is_refix = 0