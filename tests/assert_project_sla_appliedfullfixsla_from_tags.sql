-- Given a number projects with work items tagged 'nn Hour Fix'
-- When query vw_project_sla with tags not null and containing like '24 Hour Fix'
-- Expect appliedfixsla to be set to '24'
-- Inspect data after '2021-02-10' as the data appears good from this date
-- no rows returned
select
    project_id, appliedfixsla
from {{ ref('vw_project_sla' )}}
where appliedfixsla IS NOT NULL
and fixdue_date IS NULL
and report_date > '2021-02-12'::date
and project_status != 'Cancelled'
union
select
    project_id, appliedfixsla
from {{ ref('vw_project_sla' )}}
where project_id in (
    select project_id 
    from {{ ref('fact_workitem' )}} 
    where tags::text like '%Fix%'
)
and appliedfixsla IS NULL
and report_date > '2021-02-12'::date
union
select
    project_id, appliedfixsla
from {{ ref('vw_project_sla' )}}
where project_id in (
    select project_id 
    from {{ ref('fact_workitem' )}} 
    WHERE tags @> '"24 Hour Fix"'
)
and appliedfixsla != '24'
