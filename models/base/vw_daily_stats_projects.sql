{{ config(materialized='table') }}

with stats as (
    select
        report_date,
        report_year,
        report_month,
        report_day,

        -- basic aggregations
	    sum(total_projects) as total_projects,
        sum(total_workitems) as total_workitems,
        sum(total_open) as total_open,
        sum(total_closed) as total_closed,

        -- total projects opened in last 7 days
        sum(sum(total_open)) 
            over (order by report_year, report_month, report_day rows between 6 preceding and current row)
            as total_open_last_7days,
        -- total projects opened in last 14 days
        sum(sum(total_open)) 
            over (order by report_year, report_month, report_day rows between 13 preceding and current row)
            as total_open_last_14days,
        -- total projects older than 7 days on this day
        sum(sum(total_open)) over (
                ORDER BY report_year, report_month, report_day ROWS BETWEEN UNBOUNDED PRECEDING AND 7 PRECEDING
            ) as total_older_than_7days,
        -- total projects older than 14 days on this day
        sum(sum(total_open)) over (
                ORDER BY report_year, report_month, report_day ROWS BETWEEN UNBOUNDED PRECEDING AND 14 PRECEDING
            ) as total_older_than_14days,
        -- response SLA aggregations
        sum(response_within_sla) as total_response_within_sla,
        round(avg(first_response_hours)::numeric, 1) as avg_first_response_hours,
        round( (sum(response_within_sla) / NULLIF(sum(total_projects), 0)) * 100, 1) 
            as response_sla_percent,
        -- first fix SLA aggregations
        sum(first_fix_within_sla) as total_first_fix_within_sla,
        round(avg(first_fix_hours)::numeric, 1) as avg_first_fix_hours,
        round( (sum(first_fix_within_sla) / NULLIF(sum(total_projects), 0)) * 100, 1) 
            as first_fix_sla_percent,
        -- final fix SLA aggregations
        sum(final_fix_within_sla) as total_final_fix_within_sla,
        round(avg(final_fix_hours)::numeric, 1) as avg_final_fix_hours,
        round( (sum(final_fix_within_sla) / NULLIF(sum(total_projects), 0)) * 100, 1) 
            as final_fix_sla_percent
    from {{ ref('vw_project_sla') }}
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

        total_response_within_sla,
        response_sla_percent,
        avg_first_response_hours,

        total_first_fix_within_sla,
        avg_first_fix_hours,
        first_fix_sla_percent,

        total_final_fix_within_sla,
        avg_final_fix_hours,
        final_fix_sla_percent
    from stats
        left outer join dates on dates.date_day = stats.report_date
)
select * from daily_projects_stats
