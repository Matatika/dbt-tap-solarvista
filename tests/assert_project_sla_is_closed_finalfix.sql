-- Given a closed project
-- When query vw_project_sla
-- Expect finalfix_date to always be set
select
    project_id
from {{ ref('vw_project_sla' )}}
left join {{ ref('dim_project' )}} dim_project
    on dim_project.reference = vw_project_sla.project_id
where is_closed = 1
and status = 'Closed'
group by 1
having min(finalfix_date) is null
