{% snapshot dim_project_snapshot %}

{{
    config(
      target_schema=var('schema'),
      unique_key='reference',
      strategy='check',      
      check_cols=['status', 'createdon', 'closedon'],
    )
}}

with projects as (
    select * from "{{var('schema')}}".project_stream
),
dim_project_snapshot as (
    select
        {{ dbt_utils.surrogate_key(['reference']) }} as project_sk,
        *
    from projects
    where status != 'Discarded'
)
select * from dim_project_snapshot
{% endsnapshot %}