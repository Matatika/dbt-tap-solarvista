-- Assert that the vw_hb_open_wo is displaying showing the correct (and minimum) amount of closed
-- WIs

select
    count(*)
from {{ ref('dim_project' )}} dp
left join {{ ref ('fact_workitem') }} fw on fw.project_sk = dp.project_sk
where dp.status = 'Active'
and dp.customer_id='SVHB'
and dp.project_type = 'Repair'
and not exists (select *
                  from {{ ref('fact_workitem')}} fw2
                  where fw2.project_sk = fw.project_sk
                  and fw2.last_modified > fw.last_modified
                  and fw2.current_stage != 'Closed')
and current_stage != 'Closed'
having count(*) != (select
                        count(*)
                    from {{ ref('vw_hb_open_wo' )}} hbv
                    where recent_workitem_stage != 'Closed')