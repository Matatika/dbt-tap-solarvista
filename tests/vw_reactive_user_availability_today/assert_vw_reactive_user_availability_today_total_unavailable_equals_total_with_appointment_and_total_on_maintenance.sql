-- Given users with appointments for today
-- Given users with 'Maintenance' assigned for today
-- Expect only these users to be Unavailable in vw_reactive_user_availability_today
-- Assert that vw_reactive_user_availability_today return no other results
select
    user_id
from {{ ref('vw_reactive_user_availability_today')}} as vw_reactive_user_availability_today
where current_availability = 'Unavailable'
and not exists (select *
                    from {{ ref('fact_appointment')}} as fact_appointment
                    where "start"::date = current_date
                    and "start" <= now() AT TIME ZONE 'BST'
--                    and "end" > now()
                    and fact_appointment.user_id = vw_reactive_user_availability_today.user_id)
and not exists (select *
                    from {{ ref('fact_user_assignment')}}
                    where to_timestamp is null
                    and fact_user_assignment.from_timestamp::date = current_date
                    and fact_user_assignment.from_timestamp <= now() AT TIME ZONE 'BST'
                    and fact_user_assignment.user_id = vw_reactive_user_availability_today.user_id
                    and reason in ('Maintenance'))