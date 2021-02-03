{{ config(materialized='table') }}

-- ensure a row is produced for every customer on every report date
-- even when there are no projects for some customers
with report_dates as (
    select distinct report_date
    from {{ ref('vw_project_sla') }}
    group by report_date
),

customers_report_dates as (
	select reference, report_date
	from report_dates
	cross join {{ ref('dim_customer') }} c
),

project_slas as (
     select * from {{ ref('vw_project_sla') }}
     where responsedue_date is not NULL
     or fixdue_date is not NULL
),

daily_stats as (
    select
        crds.report_date,
        report_year,
        report_month,
        report_day,
        crds.reference as customer_id,

        -- basic aggregations
        sum(total_projects) as total_projects,
        sum(total_workitems) as total_workitems,
        sum(response_within_sla) as response_within_sla,
        sum(first_fix_within_sla) as first_fix_within_sla,
        sum(final_fix_within_sla) as final_fix_within_sla,
		sum(response_hours) as response_hours,
		sum(first_fix_hours) as first_fix_hours,
		sum(final_fix_hours) as final_fix_hours,

        -- Total rolling open work orders on this report_date
        (select count(*)
            from {{ ref('vw_project_sla') }} p
            where p.createdon::date <= crds.report_date
            and p.final_fix::date > crds.report_date
            and p.customer_id = crds.reference
            or p.project_id in 
                (select p2.project_id 
                    from {{ ref('vw_project_sla') }} p2
                    where p2.createdon::date <= crds.report_date
                    and p2.customer_id = crds.reference
                    and p2.is_open = 1)
        ) as total_open,

        -- Total work orders closed on this report_date
        (select count(*)
            from {{ ref('vw_project_sla') }} p
            where p.closedon::date = crds.report_date
            and p.customer_id = crds.reference
        ) as total_closed,

        -- Total work orders first attended on this report_date
        (select count(*)
            from {{ ref('vw_project_sla') }} p
            where p.preworking_timestamp::date = crds.report_date
            and p.customer_id = crds.reference
        ) as total_attended,

        -- Total work orders that are to be included in response sla calculation on this report_date
        (select count(*)
            from {{ ref('vw_project_sla') }} p
            where p.createdon::date = crds.report_date
            and p.customer_id = crds.reference
            and p.appliedresponsesla is not null
        ) as total_with_response_sla

    from customers_report_dates crds
    left join project_slas vps 
        on crds.report_date = vps.report_date
        and crds.reference = vps.customer_id
    group by crds.reference, crds.report_date, report_year, report_month, report_day
    order by report_year ASC, report_month ASC, report_day ASC
),

aggregations as (
    select
        report_date,
        report_year,
        report_month,
        report_day,
        customer_id,

        -- basic aggregations
	    sum(total_projects) as total_projects,
        sum(total_workitems) as total_workitems,
        sum(total_open) as total_open,
        sum(total_projects) as total_opened,
        sum(total_closed) as total_closed,
        sum(total_attended) as total_attended,
        sum(total_with_response_sla) as total_with_response_sla,

        --
        -- These aged / rolling totals are not correct, they are meant to be the number open older than 14 days on the report_date
        -- They are instead, the number open x days ago, as the world stands now.
        -- Plus the createdon date is NULL in some records, throwing the totals off
        --
        -- rolling totals
        sum(total_open) as total_rolling_open,
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
            ) as total_open_older_than_7days,
        -- total projects older than 14 days on this day
        sum(sum(total_open)) over (
                ORDER BY report_year, report_month, report_day ROWS BETWEEN UNBOUNDED PRECEDING AND 14 PRECEDING
            ) as total_open_older_than_14days,
        -- response SLA aggregations
        sum(response_within_sla) as total_response_within_sla,
        round( avg(response_hours)::numeric, 1) as avg_first_response_hours,
        round( ((sum(response_within_sla) / NULLIF(sum(total_with_response_sla), 0)) * 100)::numeric, 2) 
            as response_sla_percent,
        -- first fix SLA aggregations
        sum(first_fix_within_sla) as total_first_fix_within_sla,
        round( avg(first_fix_hours)::numeric, 1) as avg_first_fix_hours,
        round( ((sum(first_fix_within_sla) / NULLIF(sum(total_projects), 0)) * 100)::numeric, 2) 
            as first_fix_sla_percent,
        -- final fix SLA aggregations
        sum(final_fix_within_sla) as total_final_fix_within_sla,
        round( avg(final_fix_hours)::numeric, 1) as avg_final_fix_hours,
        round( ((sum(final_fix_within_sla) / NULLIF(sum(total_projects), 0)) * 100)::numeric, 2) 
            as final_fix_sla_percent
    from daily_stats
    group by report_date, report_year, report_month, report_day, customer_id
    order by report_year ASC, report_month ASC, report_day ASC
),

dates as (
    select * from {{ ref('dim_date') }}
),

customers as (
     select distinct * from {{ ref('dim_customer') }}
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
        dates.week_key,
        dates.week_of_year,

        customer_id,
        customers.name as customer_name,

        total_projects,
        total_workitems,
        total_open,
        total_closed,
        total_attended,
        total_with_response_sla,
        total_rolling_open,
        total_open_last_7days,
        total_open_last_14days,
        total_open_older_than_7days,
        total_open_older_than_14days,

        total_response_within_sla,
        response_sla_percent,
        avg_first_response_hours,

        total_first_fix_within_sla,
        avg_first_fix_hours,
        first_fix_sla_percent,

        total_final_fix_within_sla,
        avg_final_fix_hours,
        final_fix_sla_percent
    from aggregations
        left outer join dates on dates.date_day = aggregations.report_date
        left outer join customers on customers.reference = aggregations.customer_id
)
select * from daily_projects_stats
