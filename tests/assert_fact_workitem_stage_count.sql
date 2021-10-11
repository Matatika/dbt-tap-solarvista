--Compare workitem count stage wise with total workitems - to check duplicate workitem numbers or 
--null data rows which could be error records from source
select 'Workitem stage count error' as workitemtotal from 
(select distinct count(work_item_id) as workitemtotal from {{ ref('fact_workitem' )}}) workitemtotal,
(select count(work_item_id) as drafts from {{ ref('fact_workitem' )}} where current_stage 
in ('Drafting')) drafts,
(select count(work_item_id) as asnd from {{ ref('fact_workitem' )}} where current_stage 
in ('Accepted','Assigned')) asnd,
(select count(work_item_id) as oth from {{ ref('fact_workitem' )}} where current_stage 
in ('Working','Cancelled','Unassigned','Discarded','Rejected','PreWorking','PostWorking','TravellingTo','TravellingFrom')) oth,
(select count(work_item_id) as billing from {{ ref('fact_workitem' )}} where current_stage 
in ('Generate Invoice')) billing,
(select count(work_item_id) as clsd from {{ ref('fact_workitem' )}} where current_stage 
in ('Closed','QuickClose','RemoteClosed')) clsd
where workitemtotal <> drafts + asnd + oth + billing + clsd