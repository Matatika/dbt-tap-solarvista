-- Given a number of closed projects
-- Given some projects with refix (reactivation)
-- Given some projects without refix (fixed first attendance)
-- When sum refix and firstfix
-- Expect equal to total number of closed projects
select
    project_id,
    0
from {{ ref('vw_project_sla' )}}
where is_refix = 0
and is_firstfix = 0
and is_closed = 1
union
select
    project_id,
    sum(is_refix) + sum(is_firstfix) as total
from {{ ref('vw_project_sla' )}}
where is_closed = 1
group by 1
having not(sum(is_refix) + sum(is_firstfix) = sum(is_closed))
