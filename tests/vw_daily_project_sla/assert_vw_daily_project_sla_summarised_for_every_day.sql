-- Given a number of projects
-- Given a year
-- When select results 
-- Expect that a summarised result is returned for every day of that year
--
select
    date_day
from {{ ref('dim_date' ) }} as dim_date
    cross join {{ ref('dim_customer' ) }} as dim_customer
where dim_date.date_year = '2021'
and not exists (
	select
		*
	from {{ ref('vw_daily_project_sla' ) }} as vw_daily_project_sla
	where vw_daily_project_sla.date_day = dim_date.date_day
	and vw_daily_project_sla.customer_id = dim_customer.reference
)
