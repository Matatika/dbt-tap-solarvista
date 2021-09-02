{{ config(materialized='table') }}

with projects as (
    select * from {{ ref('dim_project_snapshot') }} 
    where dbt_valid_to is null
    and status != 'Discarded'
),
dim_equipment as (
    select * from {{ ref('dim_equipment') }}
),

dim_project as (
    select
        projects.*
        , dim_equipment.equipment_sk
    from projects
    left join dim_equipment on dim_equipment.asset_number = projects.assetnumber 
)

select * from dim_project