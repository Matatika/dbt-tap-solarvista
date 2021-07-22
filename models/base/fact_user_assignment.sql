{{ config(materialized='table') }}

with fact_workitem as (
    select * 
    from {{ ref('fact_workitem') }}
    where schedule_start_date >= current_date - interval '30' day
),
fact_workitem_stages as (
    select *
    from {{ ref('fact_workitem_stages')}}
    where last_modified >= current_date - interval '30' day
),
fact_appointment as (
    select * 
    from {{ ref('fact_appointment') }}
    where "end"::date >= current_date - interval '30' day
),
dim_project as (
    select * from {{ ref('dim_project') }}
),
all_active_workitems as (
    select
        work_item_id
    from fact_workitem
    where current_stage not in ('Closed', 'Cancelled', 'RemoteClosed', 'Discarded', 'Rejected', 'Unassigned')
),
users_with_appointments as (
    select
        fact_appointment.user_sk as user_sk
        , fact_appointment.user_id as user_id
        , fact_appointment."start" as from_timestamp
        , case when fact_appointment."end" > now() AT TIME ZONE 'BST' then NULL else fact_appointment."end" end as to_timestamp
        , fact_appointment.appointment_id as appointment_id
        , NULL as work_item_id
        , 'Appointment' as template_display_name
        , fact_appointment.label as reason
        , fact_appointment."end" as scheduled_to_time
    from fact_appointment
),
users_with_work_items_not_non_productive as (
    select
        fact_workitem.users_sk as user_sk
        , fact_workitem.assigned_user_id as user_id
        , case when fact_workitem_stages.accepted_timestamp notnull then fact_workitem_stages.accepted_timestamp
        else fact_workitem.schedule_start_time end as from_timestamp
        , case when fact_workitem.work_item_id in (select * from all_active_workitems) then NULL
        else fact_workitem.last_modified end as to_timestamp
        , NULL as appointment_id
        , fact_workitem.work_item_id as work_item_id
        , fact_workitem.template_display_name as template_display_name
        , dim_project.project_type as reason
        , fact_workitem.schedule_start_time + (fact_workitem.schedule_duration_minutes * interval '1' minute) as scheduled_to_time
    from fact_workitem
    left join dim_project on dim_project.project_sk = fact_workitem.project_sk
    left join fact_workitem_stages on fact_workitem_stages.work_item_id = fact_workitem.work_item_id
    where template_display_name in ('Work Order / Job', 'Work Order / PPM', 'Work Order / Verisae')
    and assigned_user_id notnull
    and fact_workitem.schedule_start_time notnull
),
users_with_non_productive_work_item as (
    select
        fact_workitem.users_sk as user_sk
        , fact_workitem.assigned_user_id as user_id
        , case when fact_workitem_stages.accepted_timestamp notnull then fact_workitem_stages.accepted_timestamp
        else fact_workitem.schedule_start_time end as from_timestamp
        , case when fact_workitem.work_item_id in (select * from all_active_workitems) then NULL
        else fact_workitem.last_modified end as to_timestamp
        , NULL as appointment_id
        , fact_workitem.work_item_id as work_item_id
        , fact_workitem.template_display_name as template_display_name
        , 'Non Productive' as reason
        , fact_workitem.schedule_start_time + (fact_workitem.schedule_duration_minutes * interval '1' minute) as scheduled_to_time
    from fact_workitem
    left join fact_workitem_stages on fact_workitem_stages.work_item_id = fact_workitem.work_item_id
    where template_display_name not in ('Work Order / Job', 'Work Order / PPM', 'Work Order / Verisae')
    and fact_workitem.assigned_user_id notnull
    and fact_workitem.schedule_start_time notnull
),
final as (
    select
        *
    from users_with_non_productive_work_item
    union
    select
        *
    from users_with_appointments
    union
    select
        *
    from users_with_work_items_not_non_productive
)
select * from final
order by from_timestamp