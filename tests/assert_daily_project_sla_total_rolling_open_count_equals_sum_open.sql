-- Given a number of open projects in vw_project_sla
-- When select max daily project sla
-- Expect total open to be equal
select
    report_date,
    sum(total_rolling_open)
from {{ ref('vw_daily_project_sla' ) }}
where report_date in (select max(report_date) from {{ ref('vw_daily_project_sla' ) }})
group by 1
having not(sum(total_rolling_open) = (select sum(total_open) from {{ ref('vw_project_sla' ) }} vps))
