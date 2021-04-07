-- Assert that all Assigned users in vw_reactive_user_availability_today with no reason or template_display_name
-- have no appointments or work items scheduled now()
select
    user_id
from {{ ref('vw_reactive_user_availability_today')}}
where current_availability = 'Available'
and reason isnull
and template_display_name isnull
and user_id not in (select assigned_user_id
                    from {{ ref('fact_workitem')}}
                    where schedule_start_date = current_date
                    and schedule_start_time <= now()
                    and assigned_user_id notnull
                    and current_stage not in ('Closed', 'Cancelled', 'RemoteClosed', 'Discarded', 'Rejected', 'Unassigned'))
and user_id not in (select user_id
                    from {{ ref('fact_appointment')}}
                    where "start"::date = current_date
                    and "start" <= now()
                    and "end" > now())
group by user_id
having user_id not in (select
                            user_id
                        from {{ ref('vw_reactive_user_availability_today')}}
                        where current_availability = 'Available'
                        and reason isnull
                        and template_display_name isnull)
                                            