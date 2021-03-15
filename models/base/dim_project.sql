{{ config(materialized='table') }}

with dim_project as (
    select * from {{ ref('dim_project_snapshot') }} 
    where dbt_valid_to is null
)

select * from dim_project