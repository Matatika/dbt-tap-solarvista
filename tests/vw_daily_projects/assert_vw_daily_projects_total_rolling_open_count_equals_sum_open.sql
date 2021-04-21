-- Given a number of open projects in vw_project_sla
-- When select max daily project sla
-- Expect total open to be equal
select
    report_date,
    sum(total_open)
from {{ ref('vw_daily_projects' ) }}
where report_date in (select max(report_date) from {{ ref('vw_daily_projects' ) }})
group by 1
having not(sum(total_rolling_open) = (select count(*) from {{ ref('dim_project' ) }} where status = 'Active'))
