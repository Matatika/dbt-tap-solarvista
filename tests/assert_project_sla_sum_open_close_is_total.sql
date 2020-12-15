-- Given a number of projects
-- Given some open projects
-- Given some closed projects
-- When sum open and close
-- Expect equal to total number of projects
select
    project_id,
    sum(total_open) + sum(total_closed) as total_open_close
from {{ ref('vw_project_sla' )}}
group by 1
having not(sum(total_open) + sum(total_closed) = sum(total_projects))
