-- Given a number of projects in project_stream
-- When count dim_project
-- Expect count to be equal
select
    reference
from {{ ref('project_stream' ) }}
group by 1
where reference not in (select reference from ref('dim_project' ))
