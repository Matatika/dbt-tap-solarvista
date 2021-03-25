-- Assert that there are no values in the reason column that are NULL
select
    *
from {{ ref('fact_user_assignment')}}
where reason isnull