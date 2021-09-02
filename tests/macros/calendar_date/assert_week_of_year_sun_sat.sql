-- Given a known set of dates
-- When query with week
-- Expect date to return correct week number
select
    date_day
    , sun_sat_week_of_year
	, sun_sat_week_key
from {{ ref('dim_date' )}}
where date_day IN ('2020-12-31', '2021-01-01', '2021-01-02')
group by date_day, sun_sat_week_of_year, sun_sat_week_key
having sun_sat_week_of_year != 53 or sun_sat_week_key != '202053'
union
select
    date_day
    , sun_sat_week_of_year
	, sun_sat_week_key
from {{ ref('dim_date' )}}
where date_day IN ('2021-01-03', '2021-01-04', '2021-01-05', '2021-01-06', '2021-01-07', '2021-01-08', '2021-01-09')
group by date_day, sun_sat_week_of_year, sun_sat_week_key
having sun_sat_week_of_year != 1 or sun_sat_week_key != '202101'
union
select
    date_day
    , sun_sat_week_of_year
	, sun_sat_week_key
from {{ ref('dim_date' )}}
where date_day IN ('2021-01-10')
group by date_day, sun_sat_week_of_year, sun_sat_week_key
having sun_sat_week_of_year != 2 or sun_sat_week_key != '202102'
