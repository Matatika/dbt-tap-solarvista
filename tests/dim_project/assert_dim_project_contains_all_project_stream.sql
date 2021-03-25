-- Given a number of projects in project_stream
-- When select from dim_project
-- Expect to contain all projects that are not 'Discarded' (returning results is a test failure)
select reference, status from {{ env_var('TARGET_POSTGRES_SCHEMA') }}.project_stream
where not exists (
	select reference from {{ ref('dim_project' )}} where dim_project.reference = project_stream.reference
)
and status != 'Discarded'
