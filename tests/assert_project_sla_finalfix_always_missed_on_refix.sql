-- Assert that if a project is_refix (recalled) then the project has missed its fullfixsla.

select
    project_id
from {{ ref('vw_project_sla')}}
where is_refix = 1
and fixduedate notnull
and final_fix_missed_sla = 0
