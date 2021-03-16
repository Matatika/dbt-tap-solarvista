-- Given a closed project
-- When query vw_project_sla
-- Expect finalfix_date to always be set
select
    project_id
from {{ ref('vw_project_sla' )}}
where is_closed = 1
group by 1
having min(finalfix_date) is null
