-- Given a number projects with work items tagged 'nn Hour Fix'
-- When query vw_project_sla with tags not null and containing like '24 Hour Fix'
-- Expect appliedfullfixsla to be set to '24
-- no rows returned
select
    project_id, appliedfullfixsla
from {{ ref('vw_project_sla' )}}
where appliedfullfixsla IS NOT NULL
and fixdue_date IS NULL
and report_date > '2020-12-03'::date
and project_status != 'Cancelled'
union
select
    project_id, appliedfullfixsla
from {{ ref('vw_project_sla' )}}
where project_id in (
    select project_id 
    from {{ ref('fact_workitem' )}} 
    where tags::text like '%Fix%'
)
and appliedfullfixsla IS NULL
union
select
    project_id, appliedfullfixsla
from {{ ref('vw_project_sla' )}}
where project_id in (
    select project_id 
    from {{ ref('fact_workitem' )}} 
    WHERE tags @> '"24 Hour Fix"'
)
and appliedfullfixsla != '24'
