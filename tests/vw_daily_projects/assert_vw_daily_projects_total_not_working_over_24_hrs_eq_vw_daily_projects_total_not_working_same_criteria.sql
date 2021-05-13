-- Assert the total not working from vw_daily_projects equals to the same in dim_project_snapshot over the last 5 days

select
    report_date
    , sum(total_not_working_over_24_hrs)
from {{ ref('vw_daily_projects') }} vw_daily_projects
where report_date >= current_date - 5  -- works for all dates, but this selection reduces the test execution time
and report_date <= current_date
group by report_date
having not sum(total_not_working_over_24_hrs) = (select count(distinct(project_sk))
                                                    from {{ ref('dim_project_snapshot') }} other
                                                    where status = 'Active'
                                                    and operationalstatus = 'Not Working'
                                                    and dbt_valid_from::date <= vw_daily_projects.report_date
                                                    and (dbt_valid_to::date > vw_daily_projects.report_date or dbt_valid_to is null)
													and other.createdon::date < vw_daily_projects.report_date - 1)
