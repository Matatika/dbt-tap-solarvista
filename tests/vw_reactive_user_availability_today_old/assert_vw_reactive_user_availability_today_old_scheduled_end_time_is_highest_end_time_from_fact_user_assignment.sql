-- Assert that the scheduled_to_time for user is the max scheduled_to_time of any
-- appointments or assignments they are on now.
select
    scheduled_to_time
from {{ ref('vw_reactive_user_availability_today_old')}} as vw_reactive_user_availability_today
where scheduled_to_time notnull
and scheduled_to_time not in (select 
                                max(scheduled_to_time) as scheduled_to_time
                            from {{ ref('fact_user_assignment')}}
                            left join {{ ref('dim_user')}} on dim_user.user_id = fact_user_assignment.user_id
                            where fact_user_assignment.user_id = vw_reactive_user_availability_today.user_id
                            and from_timestamp::date = current_date
                            and from_timestamp <= now()
                            and to_timestamp isnull
                            and dim_user.is_assignable = true
                            and dim_user.is_reactive = true
                            and fact_user_assignment.scheduled_to_time notnull
                            group by fact_user_assignment.user_id)
