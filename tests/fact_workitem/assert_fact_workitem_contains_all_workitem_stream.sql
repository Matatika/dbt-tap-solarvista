-- Given a number of work items in workitem_stream
-- When select from fact_workitem
-- Expect to contain all work items that are not 'Discarded' (returning results is a test failure)
select
	workitem_stream.reference
from {{ source ('solarvista_source', 'workitem_stream') }}
left join {{ source ('solarvista_source', 'project_stream') }} on project_stream.reference = workitem_stream.properties_project_id
where (workitem_stream.properties_project_id is null or project_stream.status != 'Discarded')
and not exists (
	select fact_workitem.reference from {{ ref('fact_workitem' )}} where fact_workitem.reference = workitem_stream.reference
)
