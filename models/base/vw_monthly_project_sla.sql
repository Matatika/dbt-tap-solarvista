{{ config(materialized='table') }}

with stats as (
    select
        report_year,
        report_month,
        date_trunc('month', MIN(createdon))::date as month_start,

        -- basic aggregations
	    sum(total_projects) as total_projects,
        sum(total_workitems) as total_workitems,
        sum(total_open) as total_open,
        sum(total_closed) as total_closed,

        -- monthly aggregations
        sum(response_within_sla) as total_response_within_sla,
        round(avg(first_response_hours)::numeric, 1) as avg_first_response_hours,
        round( (sum(response_within_sla) / NULLIF(sum(total_projects), 0)) * 100, 1) 
            as response_sla_percent,
        
        sum(first_fix_within_sla) as total_first_fix_within_sla,
        round(avg(first_fix_hours)::numeric, 1) as avg_first_fix_hours,
        round( (sum(first_fix_within_sla) / NULLIF(sum(total_projects), 0)) * 100, 1) 
            as first_fix_sla_percent,
        
        sum(final_fix_within_sla) as total_final_fix_within_sla,
        round(avg(final_fix_hours)::numeric, 1) as avg_final_fix_hours,
        round( (sum(final_fix_within_sla) / NULLIF(sum(total_projects), 0)) * 100, 1) 
            as final_fix_sla_percent
    from {{ ref('vw_project_sla') }}
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

        total_response_within_sla,
        avg_first_response_hours,
        response_sla_percent,

        total_first_fix_within_sla,
        avg_first_fix_hours,
        first_fix_sla_percent

        total_final_fix_within_sla,
        avg_final_fix_hours,
        final_fix_sla_percent
    from stats
        left outer join dates on dates.date_day = stats.month_start
)
select * from monthly_projects_stats
