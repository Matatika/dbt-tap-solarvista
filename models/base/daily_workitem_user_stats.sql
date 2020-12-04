{{ config(materialized='table') }}

with stats as (
    select
        assigned_user_id,
        report_date,
        report_year,
        report_month,
        report_day,

        -- aggregations
        sum(duration_hours) as total_duration_hours,
        sum(workitem_count) as total_workitems
    from {{ ref('fact_workitem') }}
    
    group by assigned_user_id, report_date, report_year, report_month, report_day

    order by report_year ASC, report_month ASC, report_day ASC

),
users as (
    select * from {{ ref('dim_user') }}
),
dates as (
    select * from {{ ref('dim_date') }}
),

daily_workitem_user_stats as (
    select
        report_year,
        report_month,
        report_day,
        dates.day_of_week,
        dates.day_of_week_name,
        users.user_id,
        users.display_name,
    
        total_duration_hours,
        round( (total_duration_hours / lag(total_duration_hours) 
            over (ORDER BY report_year, report_month, report_day)) * 100 - 100, 1) 
            as total_duration_hours_increase_percent,

        total_workitems,
        round( (total_workitems / lag(total_workitems) 
            over (ORDER BY report_year, report_month, report_day)) * 100 - 100, 1) 
            as total_workitems_increase_percent
    from stats
    left join users on users.user_id = stats.assigned_user_id
    left outer join dates on dates.date_day = stats.report_date
)
select * from daily_workitem_user_stats