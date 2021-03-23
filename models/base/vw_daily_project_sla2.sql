--
-- A daily summarised view of customer projects w/ their type and source.
--

{{ config(materialized='table') }}

with dates as (
    select * from {{ ref('dim_date') }}
),

-- ensure a row is produced for
-- every customer
-- on every report date
-- for every project_type
-- for every source
-- even when there are no projects for some customers
customers_report_dates as (
	select
        distinct c.reference as customer_id, d.date_day, p.project_type, p.source
	from dates d
	cross join {{ ref('dim_customer') }} c
	cross join (
		select
			distinct p1.project_type, p1.source
		from {{ ref('dim_project') }} p1
	) p
),

customers as (
     select * from {{ ref('dim_customer') }}
),

projects as (
     select * from {{ ref('dim_project') }}
),

project_snapshots as (
     select * from {{ ref('dim_project_snapshot') }}
),

workitems as (
    select * from {{ ref('fact_workitem') }}
),

-- number of work items for a given project
project_workitem_count as (
    select
        workitems.project_sk,
        workitems.count as total_workitems
    from workitems
    group by workitems.project_sk
),

-- active projects on a given date
projects_opened as (
    select distinct
        customer_id, reference, project_type, source, createdon, dbt_valid_from, dbt_valid_to
    from project_snapshots
    where status = 'Active'
),

-- active projects on a given date
projects_active as (
    select distinct
        customer_id, reference, project_type, source, createdon, dbt_valid_from, dbt_valid_to
    from project_snapshots
    where status = 'Active'
),

-- Daily stats summarised by customer, type, and source
daily_stats as (
    select
        customers_report_dates.customer_id
        , customers_report_dates.date_day
        , customers_report_dates.project_type
        , customers_report_dates.source

        --
        -- basic aggregations for each day
        --
        , count(distinct projects_opened.reference) as total_projects
        , sum(total_workitems) as total_workitems
--        sum(response_within_sla) as response_within_sla,
--        sum(final_fix_within_sla) as final_fix_within_sla,
--		sum(response_hours) as response_hours,
--		sum(final_fix_hours) as final_fix_hours,

        -- Rolling totals of active projects on this date
        , count(projects_active.reference) as total_open
        , sum(case when projects_active.createdon > date_day - 7 then 1 else 0 end) as total_open_last_7days
        , sum(case when projects_active.createdon > date_day - 14 then 1 else 0 end) as total_open_last_14days
        , sum(case when projects_active.createdon < date_day - 7 then 1 else 0 end) as total_open_older_than_7days
        , sum(case when projects_active.createdon < date_day - 14 then 1 else 0 end) as total_open_older_than_14days
        -- Total work orders closed on this date
        , count(distinct projects_closed.reference) as total_closed


    from customers_report_dates
        left join projects as projects_opened
            on projects_opened.customer_id = customers_report_dates.customer_id
            and projects_opened.project_type = customers_report_dates.project_type
            and projects_opened.source = customers_report_dates.source
            and projects_opened.createdon::date = customers_report_dates.date_day
        left join projects as projects_closed
            on projects_closed.customer_id = customers_report_dates.customer_id
            and projects_closed.project_type = customers_report_dates.project_type
            and projects_closed.source = customers_report_dates.source
            and projects_closed.closedon::date = customers_report_dates.date_day
        left join project_workitem_count on project_workitem_count.project_sk = projects_opened.project_sk
        left join projects_active
            on projects_active.customer_id = customers_report_dates.customer_id
            and projects_active.project_type = customers_report_dates.project_type
            and projects_active.source = customers_report_dates.source
            and projects_active.dbt_valid_from::date <= customers_report_dates.date_day 
		    and (projects_active.dbt_valid_to::date > customers_report_dates.date_day or projects_active.dbt_valid_to is null)
    group by 
        customers_report_dates.customer_id
        , customers_report_dates.date_day
        , customers_report_dates.project_type
        , customers_report_dates.source
),

final as (
    select
        customer_id
        , daily_stats.date_day as report_date
        , dates.date_year as report_year
        , dates.date_month_of_year as report_month
        , dates.date_day_of_month as report_day
        , daily_stats.project_type
        , daily_stats.source

        , daily_stats.total_projects
        , daily_stats.total_workitems
        , daily_stats.total_closed
        , daily_stats.total_open
        , daily_stats.total_open as total_rolling_open  -- deprecated, removed from reports
        , daily_stats.total_open_last_7days
        , daily_stats.total_open_last_14days
        , daily_stats.total_open_older_than_7days
        , daily_stats.total_open_older_than_14days
--        total_attended,
--        total_reactivated,
--        total_with_response_sla,

--        total_response_within_sla,
--        avg_first_response_hours,
--        response_sla_percent,

--        total_final_fix_within_sla,
--        avg_final_fix_hours,
--        final_fix_sla_percent

        , customers.*
        , dates.*

    from daily_stats
        left join customers on customers.reference = daily_stats.customer_id
        left join dates using (date_day)
)

select * from final
