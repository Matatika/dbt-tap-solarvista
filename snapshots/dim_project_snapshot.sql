{% snapshot dim_project_snapshot %}

{{
    config(
      target_database='jvdsuiab',
      target_schema='cityfm',
      unique_key='reference',

      strategy='check',      
      check_cols=['status', 'createdon','closedon'],
    )
}}

select * from "{{var('schema')}}".project_stream 

{% endsnapshot %}