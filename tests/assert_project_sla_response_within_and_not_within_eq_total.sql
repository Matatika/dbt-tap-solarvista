-- Given a number of projects
-- Given some projects within response SLA
-- Given some projects missed response SLA
-- When sum within and missed
-- Expect equal to total number of projects
select
    project_id,
    0
from {{ ref('vw_project_sla' )}}
where response_within_sla = 0
and response_missed_sla = 0
union
select
    project_id,
    sum(response_within_sla) + sum(response_missed_sla) as total_within_missed
from {{ ref('vw_project_sla' )}}
group by 1
having not(sum(response_within_sla) + sum(response_missed_sla) = 1)
