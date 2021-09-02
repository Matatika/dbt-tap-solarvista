-- Assert that no firstfix_date(s) are less than the finalfix_date.
-- This will return nothing unless there is a firstfix_date that is greater than the finalfix_date
select
    *
from {{ ref('vw_project_sla' )}}
where finalfix_date < firstfix_date