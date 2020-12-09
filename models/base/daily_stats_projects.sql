{{ config(materialized='table') }}

with stats as (
    select
        report_date,
        report_year,
        report_month,
        report_day,

        -- aggregations
        sum(total_projects) as total_projects,
        sum(total_workitems) as total_workitems,
		sum(total_open) as total_open,
        sum(sum(total_open)) 
            over (order by report_year, report_month, report_day rows between 6 preceding and current row)
            as total_open_last_7days,
        sum(sum(total_open)) 
            over (order by report_year, report_month, report_day rows between 13 preceding and current row)
            as total_open_last_14days,
        sum(sum(total_open)) over (
                ORDER BY report_year, report_month, report_day ROWS BETWEEN UNBOUNDED PRECEDING AND 7 PRECEDING
            ) as total_older_than_7days,
        sum(sum(total_open)) over (
                ORDER BY report_year, report_month, report_day ROWS BETWEEN UNBOUNDED PRECEDING AND 14 PRECEDING
            ) as total_older_than_14days,
        sum(total_closed) as total_closed,
        round(avg(first_response_hours)::numeric, 1) as avg_first_response_hours,
        sum(response_within_sla) as total_response_within_sla,
        round(avg(final_fix_hours)::numeric, 1) as avg_final_fix_hours,
        sum(final_fix_within_sla) as total_final_fix_within_sla
    from {{ ref('stats_projects') }}
    group by report_date, report_year, report_month, report_day
    order by report_year ASC, report_month ASC, report_day ASC
),

dates as (
    select * from {{ ref('dim_date') }}
),

daily_projects_stats as (
    select
        report_date,
        report_year,
        report_month,
        report_day,
        dates.day_of_month,
        dates.day_of_year,
        dates.day_of_week,
        dates.day_of_week_name,
    
        total_projects,
        total_workitems,
        total_open,
        total_closed,
        total_open_last_7days,
        total_open_last_14days,
        total_older_than_7days,
        total_older_than_14days,
        avg_first_response_hours,
        total_response_within_sla,
        avg_final_fix_hours,
        total_final_fix_within_sla
    from stats
        left outer join dates on dates.date_day = stats.report_date
)
select * from daily_projects_stats
