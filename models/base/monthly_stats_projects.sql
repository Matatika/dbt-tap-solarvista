{{ config(materialized='table') }}

with stats as (
    select
        report_year,
        report_month,
        date_trunc('month', MIN(createdon))::date as month_start,

        -- aggregations
	    sum(total_projects) as total_projects,
        sum(total_workitems) as total_workitems,
        sum(total_open) as total_open,
        sum(total_closed) as total_closed,
        round(avg(first_response_hours)::numeric, 1) as avg_first_response_hours,
        sum(response_within_sla) as total_response_within_sla,
        round( (sum(response_within_sla) / NULLIF(sum(total_projects), 0)) * 100, 1) 
            as response_sla_percent,
        round(avg(first_fix_hours)::numeric, 1) as avg_first_fix_hours,
        sum(first_fix_within_sla) as total_first_fix_within_sla,
        round( (sum(first_fix_within_sla) / NULLIF(sum(total_projects), 0)) * 100, 1) 
            as first_fix_sla_percent,
        round(avg(final_fix_hours)::numeric, 1) as avg_final_fix_hours,
        sum(final_fix_within_sla) as total_final_fix_within_sla,
        round( (sum(final_fix_within_sla) / NULLIF(sum(total_projects), 0)) * 100, 1) 
            as final_fix_sla_percent
    from {{ ref('stats_projects') }}
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
        total_open,
        total_closed,
        avg_first_response_hours,
        total_response_within_sla,
        response_sla_percent,
        avg_first_fix_hours,
        total_first_fix_within_sla,
        first_fix_sla_percent
        avg_final_fix_hours,
        total_final_fix_within_sla,
        final_fix_sla_percent
    from stats
        left outer join dates on dates.date_day = stats.month_start
)
select * from monthly_projects_stats
