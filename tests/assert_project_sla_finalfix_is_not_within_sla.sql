-- Given a project with a fix due date
-- Given a project with a closedon date after the fix due date
-- When query vw_project_sla
-- Expect final_fix_within_sla to be equal to 0 (returning results is a test failure)
select
    project_id,
    response_within_sla
from {{ ref('vw_project_sla' )}}
where project_id = '74764427'
and final_fix_within_sla != 0
