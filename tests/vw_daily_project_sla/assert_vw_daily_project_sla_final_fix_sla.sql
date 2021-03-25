-- Given a number of projects
-- Given a number of work items
-- When select a customer and a day
-- Expect that current days final fix sla is calculated correctly
--
select
    report_date
	, customer_id
    , project_type
    , source
    , sum(total_with_final_fix_sla) "total_with_final_fix_sla"
from {{ ref('vw_daily_project_sla' ) }} vw_daily_project_sla
where report_date >= current_date - 1  -- works for all dates, but this selection reduces the test execution time
and report_date <= current_date
group by report_date, customer_id, project_type, source
having not(sum(total_with_final_fix_sla) = (
		select
			count(reference)
		from {{ ref('dim_project' ) }} dim_project
		where dim_project.createdon::date = vw_daily_project_sla.report_date
		and dim_project.customer_id = vw_daily_project_sla.customer_id
		and dim_project.project_type = vw_daily_project_sla.project_type
		and dim_project.source = vw_daily_project_sla.source
		and appliedfixsla is not null
	)
)
union
select
    report_date
	, customer_id
    , project_type
    , source
    , sum(total_final_fix_within_sla) "total_final_fix_within_sla"
from {{ ref('vw_daily_project_sla' ) }} vw_daily_project_sla
where report_date >= current_date - 1  -- works for all dates, but this selection reduces the test execution time
and report_date <= current_date
group by report_date, customer_id, project_type, source
having not(sum(total_final_fix_within_sla) = (
		select
			sum(case 
	            when dim_project.appliedfixsla is null then 0
	            when dim_project.fixduedate is null then 0
	            when is_cancelled = 1 then 1 
	            when finalfix_date is null and 
					((now()::date - dim_project.fixduedate::date) * 24 + 
              				(DATE_PART('hour', now()) - DATE_PART('hour', dim_project.fixduedate)) <= 0) then 1
	            when ((finalfix_date::date - dim_project.fixduedate::date) * 24 + 
              				(DATE_PART('hour', finalfix_date) - DATE_PART('hour', dim_project.fixduedate)) <= 0) then 1
	            else 0
	         end) as full_fix_within_sla
		from {{ ref('dim_project' ) }} dim_project
        left join (
		   select
		        dim_project2.project_sk
				, (case
					when min(dim_project2.closedon) is not null then min(dim_project2.closedon)
					when min(dim_project2.status) != 'Active' then
						case
							when max(fact_workitem_stages2.closed_timestamp) is not null 
								then max(fact_workitem_stages2.closed_timestamp)
							when min(fact_workitem2.created_on) is not null
								then min(fact_workitem2.created_on)
								else min(dim_project2.createdon)
						end
				end) as finalfix_date
			    , (case 
				     when min(dim_project2.status) = 'Cancelled' then 1 
				 end) as is_cancelled
		    from {{ ref('dim_project' ) }} as dim_project2
		        left join {{ ref('fact_workitem' ) }} as fact_workitem2
		            on fact_workitem2.project_sk = dim_project2.project_sk
		        left join {{ ref('fact_workitem_stages' ) }} as fact_workitem_stages2 using (work_item_id)
		    group by dim_project2.project_sk
        ) as project_sla on project_sla.project_sk = dim_project.project_sk
		where dim_project.createdon::date = vw_daily_project_sla.report_date
		and dim_project.customer_id = vw_daily_project_sla.customer_id
		and dim_project.project_type = vw_daily_project_sla.project_type
		and dim_project.source = vw_daily_project_sla.source
		group by dim_project.createdon::date
	)
)
