-- Given a number of closed projects on a given day
-- When select daily project sla for that report date
-- Expect total closed to be equal
select
    report_date,
    sum(total_closed)
from {{ ref('vw_daily_project_sla' ) }}
where report_date = '2020-11-11'::date
group by 1
having not(sum(total_closed) != 122)
union
-- check the last day of data
select
    report_date,
    sum(total_closed)
from {{ ref('vw_daily_project_sla' ) }}
where report_date = (select max(report_date) + 1 from {{ ref('vw_daily_project_sla' ) }})
group by 1
having not(sum(total_closed) = (
        select sum(is_closed) 
        from {{ ref('vw_project_sla' ) }} vps
        where final_fix::date = (
            select max(report_date) from {{ ref('vw_daily_project_sla' ) }}
        ) 
    )
)