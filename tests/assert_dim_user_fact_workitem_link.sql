--Check user and fact workitem links 
--No rows returned - Fact row should have user data in dimension
select fw.users_sk 
from {{ ref('dim_user' )}} du, {{ ref('fact_workitem' )}} fw
where du.users_sk = fw.users_sk 
and du.users_sk not in (select distinct users_sk from {{ ref('fact_workitem' )}})
