-- Assert than if a project has a finalfix_date then it has a firstfix_date
-- This will return nothing is there are no null firstfix_dates
select
    *
from {{ ref('vw_project_sla' )}}
where finalfix_date notnull
and firstfix_date isnull