--
-- Form a 'clean' workitem_stream
--

with source as (
    
        select * from "{{var('schema')}}".workitem_stream
    
),

project_stream as (
    select * from "{{var('schema')}}".project_stream
),

cleaned as (

	select
		source.*
	from source
	left join project_stream on project_stream.reference = source.properties_project_id
    where (
        source.properties_project_id is null -- load work items without projects (ad hoc work items)
        or project_stream.status != 'Discarded'  -- do not load work items from discarded projects
    )

),

renamed as (
    
    select
        *
    from cleaned

)

select * from renamed