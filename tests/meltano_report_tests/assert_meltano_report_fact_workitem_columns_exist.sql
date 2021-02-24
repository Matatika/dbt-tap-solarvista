select
    fact_workitem.report_year,
    fact_workitem.report_month,
    fact_workitem.report_day,
    fact_workitem.work_item_id,
    fact_workitem.customer_id,
    fact_workitem.schedule_start_date,
    fact_workitem.workitem_count,
    fact_workitem.duration_hours,
    fact_workitem.charge,
    fact_workitem.price_inc_tax
from {{ ref('fact_workitem')}}
where fact_workitem.work_item_id = null
