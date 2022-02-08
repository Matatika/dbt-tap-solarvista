-- Given a number of projects in project_stream
-- When select from dim_project
-- Expect to contain all projects that are not 'Discarded' (returning results is a test failure)
select reference, status from {{ source ('solarvista_source', 'project_stream') }}
where not exists (
	select reference from {{ ref('dim_project' )}} where dim_project.reference = project_stream.reference
)
and status != 'Discarded'
union
-- check snapshot has not already been loaded
select reference, status
from {{ ref('dim_project' )}}
where not exists (
	select reference 
	from {{ source ('solarvista_source', 'project_stream') }}
	where project_stream.reference = dim_project.reference
	and project_stream.status = dim_project.status
)
