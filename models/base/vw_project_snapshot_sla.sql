with workitems as (
    select * from {{ ref('fact_workitem') }}
),
projects as (
     select distinct * from {{ ref('dim_project') }} 
),
project_workitem_count as (
    select distinct
        projects.reference as project_id,
        workitems.count as total_workitems
    from workitems
    left join projects 
        on projects.dbt_scd_id = workitems.project_sk   
    group by projects.reference
),
project_workitem_active as (
    select distinct
        projects.reference as project_id,
        workitems.count as active_workitems
    from workitems, projects 
    where projects.dbt_scd_id = workitems.project_sk 
    and workitems.current_stage Not in ('Discarded','Closed','RemoteClosed','Rejected','Cancelled')
    group by projects.reference
),
dates as (
    select * from {{ ref('dim_date') }}
),

final as (
    select
        projects.reference as project_id,
        createdon::date as report_date,
        EXTRACT(YEAR FROM createdon)::integer as report_year,
        EXTRACT(MONTH FROM createdon)::integer as report_month,
        EXTRACT(DAY FROM createdon)::integer as report_day,
    
	    project_type, 
	    status,  
        createdon,
        closedon,
        appliedresponsesla,
        responseduedate,
        fixduedate,
        total_workitems,
        active_workitems,

        dates.day_of_month,
        dates.day_of_year,
        dates.day_of_week,
        dates.day_of_week_name,
        dates.week_key,
        dates.week_of_year

    from projects
        left outer join dates on dates.date_day = projects.createdon
        left outer join project_workitem_count on project_workitem_count.project_id = projects.reference
        left outer join project_workitem_active on project_workitem_active.project_id = projects.reference

)
select * from final
