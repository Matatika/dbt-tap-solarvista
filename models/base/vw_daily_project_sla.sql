--
-- A daily summarised view of customer projects
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
dimensions as (
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

workitem_stages as (
    select * from {{ ref('fact_workitem_stages') }}
),

workitem_stats as (
    select
        date_day, projects.customer_id, projects.project_type, projects.source

        -- Total work items created on this report_date
        , count(distinct workitems.reference) as total_workitems

        -- Total projects recalled on this report_date
        , sum(case when workitems.tags ? 'Reactivation' then 1 else 0 end) as total_reactivated

    from dates
        left join workitems
            on workitems.created_on::date = date_day
        left join workitem_stages using (work_item_id)
        left join projects
            on projects.project_sk = workitems.project_sk
    group by date_day, projects.customer_id, projects.project_type, projects.source
),

workitems_attended as (
    select
        date_day, projects.customer_id, projects.project_type, projects.source

        -- Total projects first attended on this report_date
        , count(distinct projects.reference) as total_attended
  
    from dates
        left join workitem_stages
            on workitem_stages.preworking_timestamp::date = date_day or workitem_stages.quickclose_timestamp::date = date_day
        left join workitems
            on workitem_stages.work_item_id = workitems.work_item_id
        left join projects
            on projects.project_sk = workitems.project_sk
    group by date_day, projects.customer_id, projects.project_type, projects.source
),

project_stats as (
    select
        date_day, customer_id, project_type, source

        -- projects created on a given date
        , count(reference) as total_created
        , sum(case when responseduedate is not null then 1 else 0 end) as total_with_response_sla
        , sum(case when fixduedate is not null then 1 else 0 end) as total_with_final_fix_sla
    from dates
        left join projects
            on projects.createdon::date = dates.date_day
    group by date_day, customer_id, project_type, source
),

projects_closed as (
    select
        date_day, customer_id, project_type, source
        -- projects closed on a given date
        , count(distinct reference) as total_closed
    from dates
        left join project_snapshots
            on project_snapshots.dbt_valid_from::date <= date_day 
		    and (project_snapshots.dbt_valid_to::date > date_day or project_snapshots.dbt_valid_to is null)
    where status != 'Active'
    and closedon::date = date_day
    group by date_day, customer_id, project_type, source
),

-- active projects on a given date
projects_aged_active_totals as (
    select
        date_day, customer_id, project_type, source
        , count(distinct reference) as total_open
        , sum(case when createdon > date_day - 7 then 1 else 0 end) as total_open_last_7days
        , sum(case when createdon > date_day - 14 then 1 else 0 end) as total_open_last_14days
        , sum(case when createdon < date_day - 7 then 1 else 0 end) as total_open_older_than_7days
        , sum(case when createdon < date_day - 14 then 1 else 0 end) as total_open_older_than_14days
    from dates
        left join project_snapshots
            on project_snapshots.dbt_valid_from::date <= date_day 
		    and (project_snapshots.dbt_valid_to::date > date_day or project_snapshots.dbt_valid_to is null)
    where status = 'Active'
    group by date_day, customer_id, project_type, source
),

-- Daily stats summarised by customer, type, and source
daily_stats as (
    select
        dimensions.date_day
        , dimensions.customer_id
        , dimensions.project_type
        , dimensions.source

        -- Total projects created on this report_date
        , sum(project_stats.total_created) as total_created
        -- Total projects attended on this report date
        , sum(workitems_attended.total_attended) as total_attended
        -- Total projects closed on this report_date
        , sum(projects_closed.total_closed) as total_closed
        -- Total projects that are to be included in response sla calculation
        , sum(project_stats.total_with_response_sla) as total_with_response_sla
        -- Total projects that are to be included in final fix sla calculation
        , sum(project_stats.total_with_final_fix_sla) as total_with_final_fix_sla

        -- Work item based stats
        , sum(workitem_stats.total_workitems) as total_workitems
        , sum(workitem_stats.total_reactivated) as total_reactivated

        -- Rolling totals of active projects on this date
        , sum(projects_aged_active_totals.total_open) as total_open
        , sum(projects_aged_active_totals.total_open_last_7days) as total_open_last_7days
        , sum(projects_aged_active_totals.total_open_last_14days) as total_open_last_14days
        , sum(projects_aged_active_totals.total_open_older_than_7days) as total_open_older_than_7days
        , sum(projects_aged_active_totals.total_open_older_than_14days) as total_open_older_than_14days

    from dimensions
        left join project_stats
            on project_stats.date_day = dimensions.date_day
            and project_stats.customer_id = dimensions.customer_id
            and project_stats.project_type = dimensions.project_type
            and project_stats.source = dimensions.source
        left join workitem_stats
            on workitem_stats.date_day = dimensions.date_day
            and workitem_stats.customer_id = dimensions.customer_id
            and workitem_stats.project_type = dimensions.project_type
            and workitem_stats.source = dimensions.source
        left join workitems_attended
            on workitems_attended.date_day = dimensions.date_day
            and workitems_attended.customer_id = dimensions.customer_id
            and workitems_attended.project_type = dimensions.project_type
            and workitems_attended.source = dimensions.source
        left join projects_closed
            on projects_closed.date_day = dimensions.date_day
            and projects_closed.customer_id = dimensions.customer_id
            and projects_closed.project_type = dimensions.project_type
            and projects_closed.source = dimensions.source
        left join projects_aged_active_totals
            on projects_aged_active_totals.date_day = dimensions.date_day
            and projects_aged_active_totals.customer_id = dimensions.customer_id
            and projects_aged_active_totals.project_type = dimensions.project_type
            and projects_aged_active_totals.source = dimensions.source
    group by dimensions.date_day
        , dimensions.customer_id
        , dimensions.project_type
        , dimensions.source
),

final as (
    select
        daily_stats.date_day as report_date
        , dates.date_year as report_year
        , dates.date_month_of_year as report_month
        , dates.date_day_of_month as report_day
        , daily_stats.customer_id
        , daily_stats.project_type
        , daily_stats.source

        , daily_stats.total_created as total_projects  -- deprecated, remove from reports
        , daily_stats.total_created
        , daily_stats.total_attended
        , daily_stats.total_closed
        , daily_stats.total_workitems
        , daily_stats.total_reactivated
        , daily_stats.total_open
        , daily_stats.total_open as total_rolling_open  -- deprecated, remove from reports
        , daily_stats.total_open_last_7days
        , daily_stats.total_open_last_14days
        , daily_stats.total_open_older_than_7days
        , daily_stats.total_open_older_than_14days

        , daily_stats.total_with_response_sla
        , 0 as total_response_within_sla  -- deprecated, remove from reports
        , 0 as total_response_missed_sla  -- deprecated, remove from reports
        , 0 as avg_first_response_hours  -- deprecated, remove from reports
        , 0 as response_sla_percent  -- deprecated, remove from reports

        , daily_stats.total_with_final_fix_sla
        , 0 as total_final_fix_within_sla  -- deprecated, remove from reports
        , 0 as total_final_fix_missed_sla  -- deprecated, remove from reports
        , 0 as avg_final_fix_hours  -- deprecated, remove from reports
        , 0 as final_fix_sla_percent  -- deprecated, remove from reports

        , customers.name as customer_name
        , dates.*

    from daily_stats
        left join customers on customers.reference = daily_stats.customer_id
        left join dates using (date_day)
)

select * from final
