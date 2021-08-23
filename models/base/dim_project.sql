{{ config(materialized='table') }}

with projects as (
    select * from {{ ref('dim_project_snapshot') }} 
    where dbt_valid_to is null
    and status != 'Discarded'
),
equipments as (
    select * from "{{var('schema')}}".equipment_stream
),

dim_project as (
    select
        projects.*
    from projects
)

select * from dim_project