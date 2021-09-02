-- Given a number of closed projects
-- Given some projects within SLA
-- Given some projects missed SLA
-- When sum within and missed
-- Expect equal to total number of closed projects
select
    project_id,
    0
from {{ ref('vw_project_sla' )}}
where fixduedate is not null
and final_fix_within_sla = 0
and final_fix_missed_sla = 0
and is_closed = 1
union
select
    project_id,
    sum(final_fix_within_sla) + sum(final_fix_missed_sla) as total_within_missed
from {{ ref('vw_project_sla' )}}
where fixduedate is not null
and is_closed = 1
group by 1
having not(sum(final_fix_within_sla) + sum(final_fix_missed_sla) = sum(is_closed))
