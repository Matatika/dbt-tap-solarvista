{{ config(materialized='table') }}

with daily_stats as (
    select
        report_date,
        report_year,
        report_month,
        report_day,
        date_trunc('month', MIN(createdon))::date as month_start,

        -- basic aggregations
	    sum(total_projects) as total_projects,
        sum(total_workitems) as total_workitems,
        sum(response_within_sla) as response_within_sla,
        sum(first_fix_within_sla) as first_fix_within_sla,
        sum(final_fix_within_sla) as final_fix_within_sla,
		sum(response_hours) as response_hours,
		sum(first_fix_hours) as first_fix_hours,
		sum(final_fix_hours) as final_fix_hours,

        -- Calculate the total number of open work orders on this report_date
        (
            select count(*)
            from {{ ref('vw_project_sla') }} p
            where vps.report_date between p.createdon and p.remoteclosed_timestamp
            or vps.report_date between p.createdon and p.quickclose_timestamp
            or vps.report_date between p.createdon and p.closed_timestamp
            or p.is_open = 1
        ) as total_open,

        -- Calculate the number of work orders closed on this report_date
        (
            select count(*)
            from {{ ref('vw_project_sla') }} p
            where p.remoteclosed_timestamp::date = vps.report_date
            or p.quickclose_timestamp::date = vps.report_date
            or p.closed_timestamp::date = vps.report_date
        ) as total_closed
    from {{ ref('vw_project_sla') }} vps
    group by report_date, report_year, report_month, report_day
    order by report_year ASC, report_month ASC, report_day ASC
),

monthly_stats as (
    select
        report_year,
        report_month,
        min(month_start) as month_start,

        -- basic aggregations
	    sum(total_projects) as total_projects,
        sum(total_workitems) as total_workitems,
        sum(total_open) as total_open,
        sum(total_closed) as total_closed,

        -- monthly aggregations
        sum(response_within_sla) as total_response_within_sla,
        round( avg(response_hours)::numeric, 1) as avg_first_response_hours,
        round( ((sum(response_within_sla) / NULLIF(sum(total_projects), 0)) * 100)::numeric, 2) 
            as response_sla_percent,
        
        sum(first_fix_within_sla) as total_first_fix_within_sla,
        round( avg(first_fix_hours)::numeric, 1) as avg_first_fix_hours,
        round( ((sum(first_fix_within_sla) / NULLIF(sum(total_projects), 0)) * 100)::numeric, 2) 
            as first_fix_sla_percent,
        
        sum(final_fix_within_sla) as total_final_fix_within_sla,
        round( avg(final_fix_hours)::numeric, 1) as avg_final_fix_hours,
        round( ((sum(final_fix_within_sla) / NULLIF(sum(total_projects), 0)) * 100)::numeric, 2) 
            as final_fix_sla_percent
    from daily_stats
    group by report_year, report_month
    order by report_year ASC, report_month ASC
),

dates as (
    select * from {{ ref('dim_date') }}
),

final as (
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
        first_fix_sla_percent,

        total_final_fix_within_sla,
        avg_final_fix_hours,
        final_fix_sla_percent
    from monthly_stats
        left outer join dates on dates.date_day = monthly_stats.month_start
)
select * from final
