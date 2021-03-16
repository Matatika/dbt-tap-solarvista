-- Assert that all unassigned users in vw_reactive_user_availability_today have no appointments or work items scheduled now()
select
    user_id
from {{ ref('vw_reactive_user_availability_today')}}
where current_availability = 'Unassigned'
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
                        where current_availability = 'Unassigned')
                                            