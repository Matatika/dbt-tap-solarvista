{{ config(materialized='table') }}

with users as (
    select * from "{{var('schema')}}".users_stream
),
dim_user as (
    select
        {{ dbt_utils.surrogate_key(['user_id']) }} as users_sk,
        *
    from users
)
select * from dim_user
