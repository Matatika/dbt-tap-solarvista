{{ config(materialized='table') }}

with sites as (
    select * from {{ source ('solarvista_source', 'site_stream') }}
),
dim_site as (
    select
        {{ dbt_utils.surrogate_key(['reference']) }} as site_sk,
        sites.name, 
        sites.nickname, 
        sites.reference, 
        sites.status, 
        sites.latitude,
        sites.longitude
    from sites
)
select * from dim_site
