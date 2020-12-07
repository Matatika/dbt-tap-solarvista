{{ config(materialized='table') }}

with territories as (
    select * from "{{var('schema')}}".territory_stream
),
dim_territory as (
    select
        {{ dbt_utils.surrogate_key(['reference']) }} as territory_sk,
        *
    from territories
)
select * from dim_territory
