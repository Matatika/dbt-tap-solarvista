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
        round(avg(first_response_hours)::numeric, 1) as avg_first_response_hours,
        sum(response_within_sla) as total_response_within_sla,
        round(avg(final_fix_hours)::numeric, 1) as avg_final_fix_hours,
        sum(final_fix_within_sla) as total_final_fix_within_sla
    from {{ ref('stats_projects') }}
	where appliedresponsesla is not null
    group by report_date, report_year, report_month, report_day
    order by report_year ASC, report_month ASC, report_day ASC
),

dates as (
    select * from {{ ref('dim_date') }}
),

daily_projects_stats as (
    select
        report_year,
        report_month,
        report_day,
        dates.day_of_week,
        dates.day_of_week_name,
    
        total_projects,
        total_workitems,
        avg_first_response_hours,
        total_response_within_sla,
        avg_final_fix_hours,
        total_final_fix_within_sla
    from stats
        left outer join dates on dates.date_day = stats.report_date
)
select * from daily_projects_stats
