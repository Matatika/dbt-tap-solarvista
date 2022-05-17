{% snapshot dim_user_snapshot %}
{{
    config(
      target_schema=var('schema'),
      unique_key='user_id',
      strategy='check',
      invalidate_hard_deletes=True,
      check_cols='all',
    )
}}
with users as (
    select * from {{ source ('solarvista_source', 'users_stream') }}
),
dim_user_snapshot as (
    select
        *
    from users
)
select * from dim_user_snapshot
{% endsnapshot %}
