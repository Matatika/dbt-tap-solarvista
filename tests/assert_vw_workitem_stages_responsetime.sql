--To validate first response hours for 71511918
--Expect result responsetime matches 45 hours
select case when responsetime <> 0 then 1 else 0 end 
from 
(select 
DATE_PART('day',responseduedate ::timestamp - accepted::timestamp) * 24 +
DATE_PART('hour',responseduedate ::timestamp - accepted::timestamp) as responsetime
from {{ ref('vw_workitem_stages') }}
where reference = '71511918') as response
where responsetime <> 45

