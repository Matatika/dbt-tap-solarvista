{{ config(materialized='table') }}

with stats as (
    select
        report_year,
        report_month,
        date_trunc('month', MIN(createdon))::date as month_start,

        -- aggregations
	    sum(total_projects) as total_projects,
        sum(total_workitems) as total_workitems,
        round(avg(first_response_hours)::numeric, 1) as avg_first_response_hours,
        sum(response_within_sla) as total_response_within_sla,
        round(avg(final_fix_hours)::numeric, 1) as avg_final_fix_hours,
        sum(final_fix_within_sla) as total_final_fix_within_sla
    from {{ ref('stats_projects') }}
	where appliedresponsesla is not null
    group by report_year, report_month
    order by report_year ASC, report_month ASC
),

dates as (
    select * from {{ ref('dim_date') }}
),

monthly_projects_stats as (
    select
        report_year, 
        report_month,
        month_start,
    
        total_projects,
        total_workitems,
        avg_first_response_hours,
        total_response_within_sla,
        avg_final_fix_hours,
        total_final_fix_within_sla
    from stats
        left outer join dates on dates.date_day = stats.month_start
)
select * from monthly_projects_stats
