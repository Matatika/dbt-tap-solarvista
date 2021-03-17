-- Assert that the total count of rows in fact_user_assignment equals count of rows in 
-- fact_appointment and fact_workitems for the last 30 days.
select
    count(*)
from {{ ref('fact_user_assignment')}}
having count(*) != (select count(*)
                    from {{ ref('fact_appointment')}}
                    where "start"::date >= current_date - interval '30' day) +
                    (select count(*)
                    from {{ ref('fact_workitem') }}
                    where schedule_start_date >= current_date - interval '30' day
                    and assigned_user_id notnull)
