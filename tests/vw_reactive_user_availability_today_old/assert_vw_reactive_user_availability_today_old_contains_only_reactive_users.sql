-- Assert vw_reactive_user_availability_today contains only reactive, assignable users
select
    user_id
from {{ ref('vw_reactive_user_availability_today_old')}}
where user_id not in (select user_id from {{ ref('dim_user')}}
                        where is_assignable = true
                        and is_reactive = true)
                        