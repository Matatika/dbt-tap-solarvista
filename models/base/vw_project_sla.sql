
with workitems_history as (
    select * from {{ ref('fact_workitem_history') }}
),
workitems as (
    select * from {{ ref('fact_workitem') }}
),
projects as (
     select distinct * from {{ ref('dim_project') }}
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
workitems_reactivated as (
--Sql to retrieve the earliest reactivation work item date
select wi.project_id, wi.created_on
FROM (
    select wi.project_id, Min(wi.created_on) AS MinDate
    from workitems wi 
    where tags ? 'Reactivation' 
    GROUP BY wi.project_id
) AS t2
INNER JOIN workitems wi ON wi.project_id = t2.project_id AND wi.created_on = t2.MinDate
),

project_dates as (
    select distinct
        projects.reference as project_id,
        min(workitems.customer_sk) as customer_sk,
        min(workitems.site_sk) as site_sk,
        min(workitems.territory_sk) as territory_sk,
	    min(projects.project_type) as project_type,
	    min(projects.status) as project_status,
        min(projects.appliedresponsesla) as appliedresponsesla,
        min(projects.responseduedate) as responsedue_date,
        min(projects.fixduedate) as fixdue_date,
        workitems.count as total_workitems,

        -- dates from workitems
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
        min(workitems_reactivated.created_on) as reactivated_timestamp,

        -- Used to calculate SLAs
        (case
             when min(workitems.created_on) is not null then min(workitems.created_on)
             when min(projects.createdon) is not null then min(projects.createdon)
		 end) as createdon,
        (case
             when max(projects.closedon) is not null then max(projects.closedon)
             when max(workitems_remoteclosed.stage_transition_received_at) is not null 
                then max(workitems_remoteclosed.stage_transition_received_at)
             when max(workitems_quickclose.stage_transition_received_at) is not null 
                then max(workitems_quickclose.stage_transition_received_at)
             when max(workitems_cancelled.stage_transition_received_at) is not null 
                then max(workitems_cancelled.stage_transition_received_at)
             when max(projects.status) != 'Active' then
                case
                    when max(workitems_closed.stage_transition_received_at) is not null 
                        then max(workitems_closed.stage_transition_received_at)
                     when min(workitems.created_on) is not null
                        then min(workitems.created_on)
                        else min(projects.createdon)
                end
		 end) as closedon
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
    left join workitems_reactivated using (project_id)
    group by projects.reference
),

project_states as (
    select distinct

        -- keys
        project_id,
        min(customer_sk) as customer_sk,
        min(site_sk) as site_sk,
        min(territory_sk) as territory_sk,

	    min(project_type) as project_type,
	    min(project_status) as project_status,
        min(createdon) as createdon,
        min(closedon) as closedon,
        min(appliedresponsesla) as appliedresponsesla,
        min(responsedue_date) as responsedue_date,
        min(fixdue_date) as fixdue_date,
        min(total_workitems) as total_workitems,

        -- dates from workitems
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
        min(reactivated_timestamp) as reactivated_timestamp,

        (case
             -- Use PreWorking time as first response or closed time
             when min(preworking_timestamp) is not null 
                then min(preworking_timestamp)
                else min(closedon)
         end ) as firstresponse_date,
        -- TODO, we dont actually have a fix date yet
        min(closedon) as firstfix_date,
        min(closedon) as finalfix_date,
	    (case 
             when min(closedon) is not null then 0
             when min(total_workitems) = 0 then 0
		     when min(project_status) = 'Active' then 1 else 0
		 end) as is_open,
        (case
             when min(closedon) is not null then 1
             when min(total_workitems) = 0 then 1
		     when min(project_status) = 'Active' then 0 else 1 
		 end) as is_closed,
	    (case 
		     when min(cancelled_timestamp) is not null then 1 
		     when min(project_status) = 'Cancelled' then 1 
		 end) as is_cancelled
    from project_dates
    group by project_id
),

stats as (
    select distinct
        project_id,
        min(customer_sk) as customer_sk,
        min(site_sk) as site_sk,
        min(territory_sk) as territory_sk,
        createdon::date as report_date,
        EXTRACT(YEAR FROM createdon)::integer as report_year,
        EXTRACT(MONTH FROM createdon)::integer as report_month,
        EXTRACT(DAY FROM createdon)::integer as report_day,

	    min(project_type) as project_type,
	    min(project_status) as project_status,
        min(createdon) as createdon,
        min(closedon) as closedon,
        min(appliedresponsesla) as appliedresponsesla,
        min(responsedue_date) as responsedue_date,
        min(fixdue_date) as fixdue_date,
        count(project_id) as total_projects,
		sum(total_workitems) as total_workitems,

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
        min(reactivated_timestamp) as reactivated_timestamp,

        min(is_open) as is_open,
        min(is_closed) as is_closed,
        min(is_cancelled)as is_cancelled,
        min(firstresponse_date) as first_response,  
		min(firstfix_date) as first_fix,
		min(finalfix_date) as final_fix,

        -- Compute "Response" SLA by comparing project 'responseduedate' with 'PreWorking' stage
        {{ dbt_utils.datediff('min(responsedue_date)', 'min(firstresponse_date)', 'hour') }} as response_hours,
		(case 
            when min(is_cancelled) = 1 then 1 
            when min(appliedresponsesla) is null then 0 
            when {{ dbt_utils.datediff('min(responsedue_date)', 'min(firstresponse_date)', 'hour') }} <= 0 then 1 else 0 
         end) as response_within_sla,
        -- Compute "First Fix" SLA by comparing project ? with '?' stage
        {{ dbt_utils.datediff('min(fixdue_date)', 'min(firstfix_date)', 'hour') }} as first_fix_hours,
               (case 
            when min(is_cancelled) = 1 then 1 
            when {{ dbt_utils.datediff('min(fixdue_date)', 'min(firstfix_date)', 'hour') }} <= 0 then 1 else 0 
         end) as first_fix_within_sla,
        -- Compute "Final Fix" SLA by comparing project 'fixduedate' with project 'closedon'
        {{ dbt_utils.datediff('min(fixdue_date)', 'min(finalfix_date)', 'hour') }} as final_fix_hours,
		(case 
            when min(is_cancelled) = 1 then 1 
            when min(fixdue_date) is null then 1 
            when {{ dbt_utils.datediff('min(fixdue_date)', 'min(finalfix_date)', 'hour') }} <= 0 then 1 else 0 
         end) as final_fix_within_sla,
		(case 
            when min(is_cancelled) = 1 then 0 
            when min(fixdue_date) is null then 0 
            when {{ dbt_utils.datediff('min(fixdue_date)', 'min(finalfix_date)', 'hour') }} > 0 then 1 else 0 
         end) as final_fix_missed_sla,
		(case 
            when min(reactivated_timestamp) is null then 1 else 0
         end) as is_firstfix,
		(case 
            when min(reactivated_timestamp) is not null then 1 else 0
         end) as is_refix
    from project_states
    where project_id is not null
    group by project_id, report_date, report_year, report_month, report_day
    order by report_year ASC, report_month ASC, report_day ASC
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
dates as (
    select * from {{ ref('dim_date') }}
),

final as (
    select
        project_id,
        report_date,
        report_year,
        report_month,
        report_day,

	    project_type,
	    project_status,
        createdon,
        closedon,
        appliedresponsesla,
        responsedue_date,
        fixdue_date,
        total_projects,
		total_workitems,

        accepted_timestamp,
        closed_timestamp,  
        assigned_timestamp,  
        cancelled_timestamp,  
--      discarded_timestamp,  
--      postworking_timestamp,  
        preworking_timestamp,  
        quickclose_timestamp,  
        remoteclosed_timestamp,  
--      travellingfrom_timestamp,  
--      travellingto_timestamp,  
--      unassigned_timestamp,  
--      as working_timestamp,
        reactivated_timestamp,

        is_open,
        is_closed,
        is_cancelled,
        is_refix,
        is_firstfix,
        first_response,  
		first_fix,
		final_fix,

        response_hours,
        response_within_sla,
        first_fix_hours,
        first_fix_within_sla,
        final_fix_hours,
        final_fix_within_sla,
        final_fix_missed_sla,

        dates.day_of_month,
        dates.day_of_year,
        dates.day_of_week,
        dates.day_of_week_name,
        dates.week_key,
        dates.week_of_year,
        territories.reference as territory_id,
        territories.name as territory_name,
        sites.reference as site_id,
        sites.name as site_name,
        customers.reference as customer_id,
        customers.name as customer_name


    from stats
        left outer join dates on dates.date_day = stats.report_date
        left outer join customers on customers.customer_sk = stats.customer_sk
        left outer join sites on sites.site_sk = stats.site_sk
        left outer join territories on territories.territory_sk = stats.territory_sk
)
select * from final
