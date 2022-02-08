{{ config(materialized='table') }}

with customers as (
    select * from {{ source ('solarvista_source', 'customer_stream') }}
),
dim_customer as (
    select
        {{ dbt_utils.surrogate_key(['reference']) }} as customer_sk,
        customers.name, 
        customers.nickname, 
        customers.reference, 
        customers.status, 
        customers.terms_days
    from customers
)
select * from dim_customer
