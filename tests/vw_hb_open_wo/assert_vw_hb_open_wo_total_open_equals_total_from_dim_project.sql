-- Assert that vw_hb_open_wo total WOs equals the amount from dim_project
-- (With the same selection criteria)

select
    count(*)
from {{ ref('dim_project' )}} dp
where dp.status = 'Active'
and dp.customer_id='SVHB'
and dp.project_type = 'Repair'
having count(*) != (select
                        count(*)
                    from {{ ref('vw_hb_open_wo' )}} hbv)