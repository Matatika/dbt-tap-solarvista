{% snapshot dim_project_snapshot %}

{{
    config(
      target_schema=var('schema'),
      unique_key='reference',
      strategy='check',      
      check_cols=['status', 'createdon', 'closedon'],
    )
}}

select * from "{{var('schema')}}".project_stream

{% endsnapshot %}