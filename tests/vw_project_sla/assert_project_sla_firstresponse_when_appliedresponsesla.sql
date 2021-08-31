-- Given a number of closed projects
-- Given a number of project with an appliedresponsesla
-- When query vw_project_sla with appliedresponsesla not null
-- Expect firstresponse_date to be set to a value
select
    project_id
from {{ ref('vw_project_sla' )}}
where appliedresponsesla IS NOT NULL
and firstresponse_date IS NULL
and is_closed = 1
and project_status != 'Cancelled'
UNION
select
    project_id
from {{ ref('vw_project_sla' )}}
where responsedue_date IS NOT NULL
and firstresponse_date IS NULL
and is_closed = 1
and project_status != 'Cancelled'
