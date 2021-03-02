-- Assert that all assigned, unassigned and unavailable engineers
-- equal total assignable from dim_user.

select
	distinct(dim_user.user_id)
from {{ ref('dim_user')}}
group by dim_user.user_id
having (SELECT 
        count(distinct assigned_users.user_id) "Assigned"
    FROM {{ ref('fact_workitem')}}
    left join {{ ref('dim_user')}} as assigned_users on assigned_users.users_sk = fact_workitem.users_sk
    left join {{ ref('dim_customer')}} as customer on customer.customer_sk = fact_workitem.customer_sk
    WHERE schedule_start_time::date = now()::date
    AND fact_workitem.customer_id='2386264880'
    and assigned_users.is_assignable = true) + 
    (SELECT 
	    count(*) "Unassigned"
    from {{ ref('dim_user')}}
    where is_assignable = true
    and user_id not in (
    select user_id
    from {{ ref('fact_workitem')}}
    left join {{ ref('dim_user')}} as assigned_users on assigned_users.users_sk = fact_workitem.users_sk
    where schedule_start_time::date = now()::date
    and fact_workitem.customer_id='2386264880'
    and user_id is not null
    union
    select
        user_id
    from {{ ref('fact_appointment')}}
    where "start"::date = now()::date
    )) + 
    (select
   	    count(distinct user_id) as "Unavailable"
    from {{ ref('fact_appointment')}}
    where "start"::date = now()::date) != (select
                                            COUNT(*)
                                            from {{ ref('dim_user')}}
                                            where is_assignable = true)