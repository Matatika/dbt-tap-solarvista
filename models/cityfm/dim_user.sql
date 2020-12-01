{{ config(materialized='table') }}

with users as (
    select * from {{ ('cityfm.users_stream') }}
),
dim_user as (    
    select
      {{ dbt_utils.surrogate_key(
      'user_id'
      ) }} as users_sk,
    users.display_name, 
    users.email, 
    users.user_id
    from users
)
select * from dim_user