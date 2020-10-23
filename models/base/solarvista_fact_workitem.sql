with source as (

    select * from "{{var('schema')}}".workitem_stream

),

renamed as (

    select
        -- dimensions
        created_on::timestamp as report_date,

        EXTRACT(YEAR FROM created_on)::text as report_year,
        EXTRACT(MONTH FROM created_on)::text as report_month,
        EXTRACT(DAY FROM created_on)::text as report_day,

        properties_site_id as site_id,
        properties_customer_id as customer_id,
        properties_currency_id as currency_id,
        properties_problem_id as problem_id,
        properties_equipment_id as equipment_id,
        assigned_user_id as assigned_user_id,
        work_item_template_id as template_id,

        -- keys
        work_item_id as id,
        reference as reference,

        -- metrics
        properties_duration_hours as duration_hours,
        properties_charge as charge,
        properties_price_inc_tax as price_inc_tax

    from source

)

select * from renamed
