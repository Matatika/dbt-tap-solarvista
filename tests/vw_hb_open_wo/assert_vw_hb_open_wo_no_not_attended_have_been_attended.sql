-- Assert that WOs that havent been Attended actually have not been attended.

select
    count(*)
from {{ ref('dim_project' )}} dp
left join {{ ref ('fact_workitem') }} fw on fw.project_sk = dp.project_sk
left join {{ ref ('fact_workitem_stages') }} fws on fws.work_item_id = fw.work_item_id
where dp.status = 'Active'
and not exists (select *
                  from {{ ref ('fact_workitem') }} fw2
                  left join {{ ref ('fact_workitem_stages') }} fws2 on fws2.work_item_id = fw2.work_item_id
                  where fw2.project_sk = fw.project_sk
                  and fw2.last_modified > fw.last_modified
                  and fws2.preworking_timestamp notnull)
and dp.customer_id='SVHB'
and dp.project_type = 'Repair'
and fws.preworking_timestamp notnull
having count(*) != (select
                        count(*)
                    from {{ ref('vw_hb_open_wo' )}} hbv
                    where attendance = 'Attended')