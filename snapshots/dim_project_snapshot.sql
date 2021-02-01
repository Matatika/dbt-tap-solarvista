{% snapshot dim_project_snapshot %}

{{
    config(
      unique_key='reference',
      strategy='check',      
      check_cols=['status', 'createdon','closedon'],
    )
}}

select * from "{{var('schema')}}".project_stream 

{% endsnapshot %}