with workitems as (
    select * from {{ ref('fact_workitem') }}
),
workitem_stages as (
    select * from {{ ref('fact_workitem_stages') }} 
),
projects as (
    select * from {{ ref('dim_project') }} 
),
customers as (
    select * from {{ ref('dim_customer') }}
),
sites as (
    select * from {{ ref('dim_site') }}
),
territories as (
    select * from {{ ref('dim_territory') }}
),
users as (
    select * from {{ ref ('dim_user') }}
),
selected_projects as (
    select
        projects.reference "workorder_reference"
        , projects.project_sk "project_sk"
        , projects.description "workorder_description"
        , projects.operationalstatus "workorder_operational_status"
        , to_char(projects.createdon, 'YYYY-MM-DD HH24:MI:SS') "workorder_created_time"
        , extract(day from (now() - projects.createdon)) "workorder_age_days"
        , territories.name "territory"
        , sites.name "site_name"
        from projects
        left join workitems on workitems.project_sk = projects.project_sk
        left join territories on territories.territory_sk = workitems.territory_sk
        left join sites on sites.site_sk = workitems.site_sk
        where not exists (select *
                  from {{ ref('fact_workitem')}} fw2
                  where fw2.project_sk = workitems.project_sk
                  and fw2.last_modified > workitems.last_modified)
        and projects.status = 'Active'
        AND projects.customer_id='SVHB'
        AND projects.project_type = 'Repair'
),
most_recently_attending_engineer as (
    select 
        project_sk
        , display_name
    from workitems
    left join users on users.user_id = workitems.assigned_user_id
    left join workitem_stages on workitem_stages.work_item_id = workitems.work_item_id
    where not exists (select *
                        from {{ ref('fact_workitem')}} fw2
                        left join {{ ref ('fact_workitem_stages')}} fws on fws.work_item_id = fw2.work_item_id
                        where fw2.project_sk = workitems.project_sk
                        and fw2.last_modified > workitems.last_modified
                        and fws.preworking_timestamp notnull)
    and workitem_stages.preworking_timestamp notnull
),
most_recent_not_closed_work_item as (
    select
        project_sk
        , tags
        , current_stage
    from workitems
    where not exists (select *
                        from {{ ref('fact_workitem')}} fw3
                        where fw3.project_sk = workitems.project_sk
                        and fw3.last_modified > workitems.last_modified
                        and fw3.current_stage != 'Closed')
    and workitems.current_stage != 'Closed'
),
most_recent_work_item as (
    select 
        project_sk
        , workitems.tags
        , workitems.current_stage
    from workitems
    where not exists (select *
                        from {{ ref('fact_workitem')}} fw4
                        where fw4.project_sk = workitems.project_sk
                        and fw4.last_modified > workitems.last_modified)
),
final as (
    select
        selected_projects.*
        , case when most_recently_attending_engineer.display_name notnull then 'Attended' else 'Not Attended' end "attendance"
        , most_recently_attending_engineer.display_name "most_recent_engineer_attended"
        , case when most_recent_not_closed_work_item.tags notnull then most_recent_not_closed_work_item.tags
        else most_recent_work_item.tags end "recent_workitem_tags"
        , case when most_recent_not_closed_work_item.current_stage notnull then most_recent_not_closed_work_item.current_stage
        else most_recent_work_item.current_stage end "recent_workitem_stage"
    from selected_projects
    left join most_recently_attending_engineer on most_recently_attending_engineer.project_sk = selected_projects.project_sk
    left join most_recent_not_closed_work_item on most_recent_not_closed_work_item.project_sk = selected_projects.project_sk
    left join most_recent_work_item on most_recent_work_item.project_sk = selected_projects.project_sk
    order by "attendance", selected_projects."workorder_created_time"
)
select * from final
