-- Assert that the 

select
    scheduled_end_time
from {{ ref('vw_reactive_user_availability_today')}}
where scheduled_end_time notnull
and scheduled_end_time not in (select scheduled_end_time from (select 
                                                                    distinct(fact_user_assignment.user_id)
                                                                    , min(scheduled_end_time) as scheduled_end_time
                                                                from {{ ref('fact_user_assignment')}}
                                                                left join {{ ref('dim_user')}} on dim_user.user_id = fact_user_assignment.user_id
                                                                where from_timestamp::date = current_date
                                                                and from_timestamp <= now()
                                                                and to_timestamp isnull
                                                                and dim_user.is_assignable = true
                                                                and dim_user.is_reactive = true
                                                                and fact_user_assignment.scheduled_end_time notnull
                                                                group by fact_user_assignment.user_id) as t2)

