{{ config(materialized='table') }}

with projects as (
    select * from "{{var('schema')}}".project_stream
),
dim_project as (
    select
        {{ dbt_utils.surrogate_key(['reference']) }} as project_sk,
        *
    from projects
)
select * from dim_project
