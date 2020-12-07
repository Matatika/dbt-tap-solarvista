{{ config(materialized='table') }}

with dim_workitem_stages as (
    SELECT *
    FROM crosstab('select distinct work_item_id, stage, max(last_received_at)
                    from {{ ref("stg_workitem_stages") }}
                    group by work_item_id, stage
                    order by work_item_id',
                  'select distinct stage from {{ ref("stg_workitem_stages") }}')              
    AS (
        work_item_id TEXT,
        {{ get_stage_name_type() }}
    )
)
select * from dim_workitem_stages
