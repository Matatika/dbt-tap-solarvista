-- Assert vw_reactive_user_availability_today contains only reactive, assignable users
select
    count(user_id)
from {{ ref('vw_reactive_user_availability_today_old')}}
having count(user_id) != (select count(user_id) from {{ ref('dim_user')}}
                                        where is_assignable = true
                                        and is_reactive = true)