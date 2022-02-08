{% snapshot dim_project_snapshot %}

{{
    config(
      target_schema=var('schema'),
      unique_key='reference',
      strategy='timestamp',
      updated_at='last_modified',
    )
}}

with projects as (
    select * from {{ source ('solarvista_source', 'project_stream') }}
),
dim_project_snapshot as (
    select
        {{ dbt_utils.surrogate_key(['reference']) }} as project_sk,
        *
    from projects
)
select * from dim_project_snapshot
{% endsnapshot %}