-- assert that number of users in vw_reactive_user_availability_today is equal to
-- count of assignable and is_reactive user from dim_user
select
    vw_reactive_user_availability_today.user_id
from {{ ref('vw_reactive_user_availability_today')}}
where vw_reactive_user_availability_today.user_id not in (select dim_user.user_id
                                                                from {{ ref('dim_user')}} 
                                                                where is_assignable = true
                                                                and is_reactive = true)
group by vw_reactive_user_availability_today.user_id