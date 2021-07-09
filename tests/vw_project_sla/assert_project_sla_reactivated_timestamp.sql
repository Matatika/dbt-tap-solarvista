-- Given a project with a 'Reactivation' tagged work item
-- When query vw_project_sla
-- Expect reactivated_timestamp to be equal first work item with 'Reactivation' tag
select
    project_id,
    reactivated_timestamp
from {{ ref('vw_project_sla' )}}
where project_id = '74864898'
and reactivated_timestamp::date != '2021-01-13 14:08:15'::date
and to_char(reactivated_timestamp, 'YYYY-MM-DD HH24:MI:SS') != '2021-01-13 14:08:15'
union
select
    project_id,
    reactivated_timestamp
from {{ ref('vw_project_sla' )}} vp
where reactivated_timestamp is not null
and reactivated_timestamp != (
    select min(created_on) 
    from {{ ref('fact_workitem' )}} fw
    where tags ? 'Reactivation'
    and fw.project_id = vp.project_id
)
