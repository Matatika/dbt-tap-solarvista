-- Given a project with a response due date
-- Given a project with a first response after the response due date
-- When query vw_project_sla
-- Expect response_within_sla to be equal to 0
select
    project_id,
    response_within_sla
from {{ ref('vw_project_sla' )}}
where project_id = '74805058'
and response_within_sla != 0
