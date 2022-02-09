-- This macro drops users_stream from your target schema.

-- We run this macro by invoking it through meltano: `meltano invoke dbt run-operation drop_users_stream_table`

-- Dropping this table ensure we only get the current active users synced from solarvista, so using our dim_user_snapshot
-- we can track and handle users that have been hard deleted
{%- macro solarvista_drop_users_stream_table() -%}
    {%- set drop_query -%}
        DROP TABLE {{ target.schema }}.users_stream
    {%- endset -%}
    {% do run_query(drop_query) %}
{%- endmacro -%}