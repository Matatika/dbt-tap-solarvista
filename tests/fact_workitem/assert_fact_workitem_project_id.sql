-- Given a work item with 'Work Order / Job' or 'Work Order / Maintenance' template type
-- Expect project_id to always be set (returning results is a test failure)
select
	work_item_id, fact_workitem.reference, created_on, current_stage, project_id, template_display_name, dim_project.status
from {{ ref('fact_workitem' )}} fact_workitem
	left join {{ ref('dim_project' )}} as dim_project on dim_project.project_sk = fact_workitem.project_sk
where project_id is null
and template_display_name in ('Work Order / Job', 'Work Order / Maintenance', 'Work Order / Verisae')
and assigned_user_id is not null  -- found one dodgy work item that is a work order type, but no project, but no user assigned so don't worry
