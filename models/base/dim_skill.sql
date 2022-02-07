with skill_stream as (
    select * from {{ source ('solarvista_source', 'skill_stream') }}
)
select * from skill_stream