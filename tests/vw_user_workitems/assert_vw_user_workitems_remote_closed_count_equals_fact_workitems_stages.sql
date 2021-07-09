-- Assert that the number of work items in vw_user_workitems that are remoteclosed
-- equals the number of remoteclosed in fact_workitem_history
select
    count(*)
from {{ ref('vw_user_workitems' )}}
where remoteclosed_user_id notnull
having count(*) != (select
                        count(*)
                        from {{ ref('fact_workitem' )}} fwi
                        left join {{ ref('fact_workitem_history' )}} fwih on fwih.work_item_id = fwi.work_item_id
                        where stage_transition_to_stage_type = 'RemoteClosed'
                        and stage_transition_transitioned_by_user_id notnull)




