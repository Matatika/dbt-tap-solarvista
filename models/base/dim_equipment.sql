{{ config(materialized='table') }}

with equipment as (
    select * from {{ source ('solarvista_source', 'equipment_stream') }}
),
dim_equipment as (
    select
        {{ dbt_utils.surrogate_key(['reference']) }} as equipment_sk,
        equipment.reference, 
        equipment.asset_number, 
        equipment.category_id, 
        equipment.category_title, 
        equipment.description,
        equipment.location
    from equipment
)
select * from dim_equipment
