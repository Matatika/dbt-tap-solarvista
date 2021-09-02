{{ config(materialized='table') }}

with appointments as (
    select * from "{{var('schema')}}".appointment_stream
),
users as (
    select * from {{ ref('dim_user') }}
),
fact_appointment as (
    select distinct 
        -- key transforms
        appointments.appointment_id as id

        -- SCD surrogate keys for join purposes in reporting layer
        ,users.users_sk as user_sk

        -- facts and metrics
        ,appointments.*

	    -- dimensions
        ,appointments.start::date as report_date
        ,EXTRACT(YEAR FROM appointments.start)::integer as report_year
        ,EXTRACT(MONTH FROM appointments.start)::integer as report_month
        ,EXTRACT(DAY FROM appointments.start)::integer as report_day
        ,users.display_name as user_name


    from appointments
	left join users on users.user_id = appointments.user_id
)
select * from fact_appointment
