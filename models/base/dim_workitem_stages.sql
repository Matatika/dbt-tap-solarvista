{{
    config(
        materialized='incremental', 
        unique_key='work_item_id'
    )
}}

with workitems_history as (
    select * from {{ ref('fact_workitem_history') }}
),

workitems as (
    select * from {{ ref('fact_workitem') }}
),

workitems_accepted as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.stage_transition_received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_history ht 
    where ht.stage_stage_type = 'Accepted' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_history ht ON ht.work_item_id = t2.work_item_id AND ht.stage_transition_received_at= t2.MinDate
),
workitems_closed as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.stage_transition_received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_history ht 
    where ht.stage_stage_type = 'Closed' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_history ht ON ht.work_item_id = t2.work_item_id AND ht.stage_transition_received_at= t2.MinDate
),
workitems_assigned as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.stage_transition_received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_history ht 
    where ht.stage_stage_type = 'Assigned' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_history ht ON ht.work_item_id = t2.work_item_id AND ht.stage_transition_received_at= t2.MinDate
),
workitems_cancelled as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.stage_transition_received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_history ht 
    where ht.stage_stage_type = 'Cancelled' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_history ht ON ht.work_item_id = t2.work_item_id AND ht.stage_transition_received_at= t2.MinDate
),
workitems_discarded as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.stage_transition_received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_history ht 
    where ht.stage_stage_type = 'Discarded' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_history ht ON ht.work_item_id = t2.work_item_id AND ht.stage_transition_received_at= t2.MinDate
),
workitems_postworking as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.stage_transition_received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_history ht 
    where ht.stage_stage_type = 'PostWorking' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_history ht ON ht.work_item_id = t2.work_item_id AND ht.stage_transition_received_at= t2.MinDate
),
workitems_preworking as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.stage_transition_received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_history ht 
    where ht.stage_stage_type = 'PreWorking' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_history ht ON ht.work_item_id = t2.work_item_id AND ht.stage_transition_received_at= t2.MinDate
),
workitems_quickclose as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.stage_transition_received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_history ht 
    where ht.stage_stage_type = 'QuickClose' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_history ht ON ht.work_item_id = t2.work_item_id AND ht.stage_transition_received_at= t2.MinDate
),
workitems_remoteclosed as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.stage_transition_received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_history ht 
    where ht.stage_stage_type = 'RemoteClosed' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_history ht ON ht.work_item_id = t2.work_item_id AND ht.stage_transition_received_at= t2.MinDate
),
workitems_travellingfrom as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.stage_transition_received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_history ht 
    where ht.stage_stage_type = 'TravellingFrom' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_history ht ON ht.work_item_id = t2.work_item_id AND ht.stage_transition_received_at= t2.MinDate
),
workitems_travellingto as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.stage_transition_received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_history ht 
    where ht.stage_stage_type = 'TravellingTo' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_history ht ON ht.work_item_id = t2.work_item_id AND ht.stage_transition_received_at= t2.MinDate
),
workitems_unassigned as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.stage_transition_received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_history ht 
    where ht.stage_stage_type = 'Unassigned' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_history ht ON ht.work_item_id = t2.work_item_id AND ht.stage_transition_received_at= t2.MinDate
),
workitems_working as (
--Sql to retrieve the earliest stage transition date
select ht.work_item_id,ht.stage_transition_received_at
FROM (
    Select ht.work_item_id, Min(ht.stage_transition_received_at) AS MinDate
    from workitems_history ht 
    where ht.stage_stage_type = 'Working' 
    GROUP BY ht.work_item_id
) AS t2
INNER JOIN workitems_history ht ON ht.work_item_id = t2.work_item_id AND ht.stage_transition_received_at= t2.MinDate
),

dim_workitem_stages as (
    select distinct 
        workitems.work_item_id,
        workitems.reference,
        workitems.last_modified,
        workitems_accepted.stage_transition_received_at as accepted_timestamp,
        workitems_closed.stage_transition_received_at as closed_timestamp,  
        workitems_assigned.stage_transition_received_at as assigned_timestamp,  
        workitems_cancelled.stage_transition_received_at as cancelled_timestamp,  
        workitems_discarded.stage_transition_received_at as discarded_timestamp,  
        workitems_postworking.stage_transition_received_at as postworking_timestamp,  
        workitems_preworking.stage_transition_received_at as preworking_timestamp,  
        workitems_quickclose.stage_transition_received_at as quickclose_timestamp,  
        workitems_remoteclosed.stage_transition_received_at as remoteclosed_timestamp,  
        workitems_travellingfrom.stage_transition_received_at as travellingfrom_timestamp,  
        workitems_travellingto.stage_transition_received_at as travellingto_timestamp,  
        workitems_unassigned.stage_transition_received_at as unassigned_timestamp,  
        workitems_working.stage_transition_received_at as working_timestamp
    from workitems
        left join workitems_accepted using (work_item_id)
        left join workitems_closed using (work_item_id)
        left join workitems_assigned using (work_item_id)
        left join workitems_cancelled using (work_item_id)
        left join workitems_discarded using (work_item_id)
        left join workitems_postworking using (work_item_id)
        left join workitems_preworking using (work_item_id)
        left join workitems_quickclose using (work_item_id)
        left join workitems_remoteclosed using (work_item_id)
        left join workitems_travellingfrom using (work_item_id)
        left join workitems_travellingto using (work_item_id) 
        left join workitems_unassigned using (work_item_id)
        left join workitems_working using (work_item_id)
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where last_modified > (select max(t2.last_modified) from {{ this }} as t2)
{% endif %}
)
select * from dim_workitem_stages
