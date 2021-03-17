-- assert that number of users in vw_reactive_user_availability_today is equal to
-- count of assignable and is_reactive user from dim_user
select
    count(*)
from {{ ref('vw_reactive_user_availability_today')}}
having count(*) != (select count(dim_user.user_id)
                    from {{ ref('dim_user')}} 
                    where is_assignable = true
                    and is_reactive = true)
