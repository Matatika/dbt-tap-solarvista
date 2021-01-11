
with workitems_history as (
    select * from {{ ref('fact_workitem_history') }}
),
workitems as (
    select * from {{ ref('fact_workitem') }}
),
projects as (
     select distinct * from {{ ref('dim_project') }}
),
customers as (
     select distinct * from {{ ref('dim_customer') }}
),
sites as (
     select distinct * from {{ ref('dim_site') }}
),
territories as (
     select distinct * from {{ ref('dim_territory') }}
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
vw_project_sla as (
    select distinct
        projects.reference as project_id,
        min(workitems.customer_sk) as customer_sk,
        min(workitems.site_sk) as site_sk,
        min(workitems.territory_sk) as territory_sk,
	    min(projects.project_type) as project_type,
	    min(projects.status) as status,
        min(projects.createdon) as createdon,
        min(projects.closedon) as closedon,
        min(projects.appliedresponsesla) as appliedresponsesla,
        min(projects.responseduedate) as responseduedate,
        min(projects.fixduedate) as fixduedate,
        workitems.count as total_workitems,

        -- aggregations
        min(workitems_accepted.stage_transition_received_at) as accepted_timestamp,
        min(workitems_closed.stage_transition_received_at) as closed_timestamp,  
        min(workitems_assigned.stage_transition_received_at) as assigned_timestamp,  
        min(workitems_cancelled.stage_transition_received_at) as cancelled_timestamp,  
--        min(workitem_stages.discarded_timestamp) as discarded_timestamp,  
--        min(workitem_stages.postworking_timestamp) as postworking_timestamp,
        min(workitems_preworking.stage_transition_received_at) as preworking_timestamp,
        min(workitems_quickclose.stage_transition_received_at) as quickclose_timestamp,  
        min(workitems_remoteclosed.stage_transition_received_at) as remoteclosed_timestamp,  
--        min(workitem_stages.travellingfrom_timestamp) as travellingfrom_timestamp,  
--        min(workitem_stages.travellingto_timestamp) as travellingto_timestamp,  
--        min(workitem_stages.unassigned_timestamp) as unassigned_timestamp,  
--        min(workitem_stages.working_timestamp) as working_timestamp,

        -- Used to calculate SLAs
        min(projects.closedon) as first_fix,
        min(projects.closedon) as final_fix,
        (case
             -- Use PreWorking time as first response or closed time
             when min(workitems_preworking.stage_transition_received_at) is not null 
                then min(workitems_preworking.stage_transition_received_at)
             when min(projects.closedon) is not null then min(projects.closedon)
             when min(workitems_remoteclosed.stage_transition_received_at) is not null 
                then min(workitems_remoteclosed.stage_transition_received_at)
             when min(workitems_quickclose.stage_transition_received_at) is not null 
                then min(workitems_quickclose.stage_transition_received_at)
             when min(workitems_cancelled.stage_transition_received_at) is not null 
                then min(workitems_cancelled.stage_transition_received_at)
         end ) as first_response,  
	    (case 
             when min(projects.closedon) is not null then 0
             when max(workitems_remoteclosed.stage_transition_received_at) > max(workitems_closed.stage_transition_received_at) then 0 
             when min(workitems_remoteclosed.stage_transition_received_at) is not null then 0 
             when min(workitems_quickclose.stage_transition_received_at) is not null then 0 
             when min(workitems_cancelled.stage_transition_received_at) is not null then 0 
             when workitems.count = 0 then 0
		     when min(projects.status) = 'Active' then 1 else 0
		 end) as is_open,
        (case
             when min(projects.closedon) is not null then 1
             when max(workitems_remoteclosed.stage_transition_received_at) > max(workitems_closed.stage_transition_received_at) then 1 
             when min(workitems_remoteclosed.stage_transition_received_at) is not null then 1 
             when min(workitems_quickclose.stage_transition_received_at) is not null then 1 
             when min(workitems_cancelled.stage_transition_received_at) is not null then 1 
             when workitems.count = 0 then 1
		     when min(projects.status) = 'Active' then 0 else 1 
		 end) as is_closed,
	    (case 
		     when min(workitems_cancelled.stage_transition_received_at) is not null then 1 
		     when min(projects.status) = 'Cancelled' then 1 
		 end) as is_cancelled
    from workitems
    left join projects 
        on projects.project_sk = workitems.project_sk
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
    group by projects.reference
),

stats as (
    select distinct
        project_id,
        createdon::date as report_date,
        EXTRACT(YEAR FROM createdon)::integer as report_year,
        EXTRACT(MONTH FROM createdon)::integer as report_month,
        EXTRACT(DAY FROM createdon)::integer as report_day,

	    min(vw_project_sla.project_type) as type,
	    min(vw_project_sla.status) as status,
        min(createdon) as createdon,
        min(closedon) as closedon,
        min(appliedresponsesla) as appliedresponsesla,
        min(responseduedate) as responsedue_date,
        min(fixduedate) as fixdue_date,
        min(accepted_timestamp) as accepted_timestamp,
        min(closed_timestamp) as closed_timestamp,  
        min(assigned_timestamp) as assigned_timestamp,  
        min(cancelled_timestamp) as cancelled_timestamp,  
--        min(discarded_timestamp) as discarded_timestamp,  
--        min(postworking_timestamp) as postworking_timestamp,  
        min(preworking_timestamp) as preworking_timestamp,  
        min(quickclose_timestamp) as quickclose_timestamp,  
        min(remoteclosed_timestamp) as remoteclosed_timestamp,  
--        min(travellingfrom_timestamp) as travellingfrom_timestamp,  
--        min(travellingto_timestamp) as travellingto_timestamp,  
--        min(unassigned_timestamp) as unassigned_timestamp,  
--        min(working_timestamp) as working_timestamp,
		min(first_response) as first_response,
		min(first_fix) as first_fix,
		min(final_fix) as final_fix,

        min(territories.reference) as territory_id,
        min(territories.name) as territory_name,
        min(sites.reference) as site_id,
        min(sites.name) as site_name,
        min(customers.reference) as customer_id,
        min(customers.name) as customer_name,

		-- aggregations
        count(project_id) as total_projects,
		sum(total_workitems) as total_workitems,
		min(is_open) as is_open,
		min(is_closed) as is_closed,
		min(is_cancelled) as is_cancelled,
        -- Compute "Response" SLA by comparing project 'responseduedate' with 'PreWorking' stage
        {{ dbt_utils.datediff('min(responseduedate)', 'min(first_response)', 'hour') }} as first_response_hours,
		(case 
            when min(is_cancelled) = 1 then 1 
            when min(appliedresponsesla) is null then 1 
            when {{ dbt_utils.datediff('min(responseduedate)', 'min(first_response)', 'hour') }} <= 0 then 1 else 0 
         end) as response_within_sla,
        -- Compute "First Fix" SLA by comparing project ? with '?' stage
        {{ dbt_utils.datediff('min(fixduedate)', 'min(first_fix)', 'hour') }} as first_fix_hours,
		(case 
            when min(is_cancelled) = 1 then 1 
            when {{ dbt_utils.datediff('min(fixduedate)', 'min(first_fix)', 'hour') }} <= 0 then 1 else 0 
         end) as first_fix_within_sla,
        -- Compute "Final Fix" SLA by comparing project 'fixduedate' with project 'closedon'
        {{ dbt_utils.datediff('min(fixduedate)', 'min(final_fix)', 'hour') }} as final_fix_hours,
		(case 
            when min(is_cancelled) = 1 then 1 
            when {{ dbt_utils.datediff('min(fixduedate)', 'min(final_fix)', 'hour') }} <= 0 then 1 else 0 
         end) as final_fix_within_sla,
		(case 
            when min(final_fix) > min(first_fix) then 1 
         end) as is_refix
    from vw_project_sla
        left outer join customers on customers.customer_sk = vw_project_sla.customer_sk
        left outer join sites on sites.site_sk = vw_project_sla.site_sk
        left outer join territories on territories.territory_sk = vw_project_sla.territory_sk
    where project_id is not null
    group by project_id, report_date, report_year, report_month, report_day
    order by report_year ASC, report_month ASC, report_day ASC
)
select * from stats
