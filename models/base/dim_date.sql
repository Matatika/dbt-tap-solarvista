{{
    config(
        materialized = 'table'
    )
}}

with date_spine as (
    -- TODO, we arbitrarily start and end, update to be relative to current date:
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('01/01/2016', 'mm/dd/yyyy')",
        end_date="to_date('01/01/2022', 'mm/dd/yyyy')"
       )
    }}
),

base_dates as (
    select
        date(d.date_day) as date_day
    from
        date_spine d
),

dates_with_prior_year_dates as (
    select
        cast(d.date_day as date) as date_day,
        cast({{ dbt_utils.dateadd('year', -1 , 'd.date_day') }} as date) as prior_year_date_day,
        cast({{ dbt_utils.dateadd('day', -364 , 'd.date_day') }} as date) as prior_year_over_year_date_day
    from
    	base_dates d
)
select
    d.date_day,
    cast({{ dbt_utils.dateadd('day', -1 , 'd.date_day') }} as date) as prior_date_day,
    cast({{ dbt_utils.dateadd('day', 1 , 'd.date_day') }} as date) as next_date_day,
    d.prior_year_date_day as prior_year_date_day,
    d.prior_year_over_year_date_day,
    cast(
            case
                when {{ date_part('dow', 'd.date_day') }} = 0 then 7
                else {{ date_part('dow', 'd.date_day') }}
            end
        as {{ dbt_utils.type_int() }}
    ) as day_of_week,

    {{ day_name('d.date_day', short=false) }} as day_of_week_name,
    {{ day_name('d.date_day', short=true) }} as day_of_week_name_short,
    cast({{ date_part('day', 'd.date_day') }} as {{ dbt_utils.type_int() }}) as day_of_month,
    cast({{ date_part('doy', 'd.date_day') }} as {{ dbt_utils.type_int() }}) as day_of_year,

    -- Week number, and a week key to sort
    cast((to_char(d.date_day, 'YYYY') || right('0' || to_char(d.date_day, 'WW'), 2)) as {{ dbt_utils.type_int() }}) as week_key,
    cast(to_char(d.date_day, 'WW') as {{ dbt_utils.type_int() }}) as week_of_year

from
    dates_with_prior_year_dates d
order by 1


