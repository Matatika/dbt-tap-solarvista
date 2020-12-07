{{ config(materialized='table') }}

with workitem_facts as (
     select * from {{ ref('fact_workitem') }}
),

workitem_stages as (
     select * from {{ ref('dim_workitem_stages') }}
),

projects as (
     select * from {{ ref('dim_project') }}
),

dates as (
    select * from {{ ref('dim_date') }}
),

workitem_stats as (
    select
        project_id,

        -- aggregations
        count(workitem_facts.work_item_id) as number_workitems,
        min(workitem_stages.assigned) as first_response

    from workitem_facts
        left join workitem_stages on workitem_stages.work_item_id = workitem_facts.work_item_id
    group by project_id
),

stats as (
    select
        reference,
        projects.createdon::date as report_date,
        EXTRACT(YEAR FROM createdon)::integer as report_year,
        EXTRACT(MONTH FROM createdon)::integer as report_month,
        EXTRACT(DAY FROM createdon)::integer as report_day,

        -- aggregations
        sum(number_workitems) as total_workitems,
        sum({{ dbt_utils.datediff('createdon', 'first_response', 'hour') }}) as total_minutes_first_response
    from workitem_stats
        left join projects on projects.reference = workitem_stats.project_id
    group by reference, report_date, report_year, report_month, report_day
    order by report_year ASC, report_month ASC, report_day ASC
	
),

stats_project_slas as (
    select
        report_year,
        report_month,
        report_day,
        dates.day_of_week,
        dates.day_of_week_name,
        projects.reference,
        projects.name,
    
        total_minutes_first_response

    from stats
        left join projects on projects.reference = stats.reference
        left outer join dates on dates.date_day = stats.report_date
)
select * from stats_project_slas
