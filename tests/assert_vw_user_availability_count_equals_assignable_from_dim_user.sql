-- Assert that count of users in vw_user_availability is equal 
-- total is_assignable from dim_user.

select
    availability_date
    , count(vw_user_availability.display_name)
from {{ ref('vw_user_availability')}}
where availability_date in (select max(availability_date) from {{ ref('vw_user_availability' ) }})
group by 1
having not count(vw_user_availability.display_name) = (select count(dim_user.user_id) from {{ ref('dim_user')}} where is_assignable = true)



