-- Get a count of SQL connections by IP address (Query 39) (Connection Counts by IP Address)

select 
	   ec.client_net_address
,	   es.program_name
,	   es.host_name
,	   es.login_name
,	   db_name(es.database_id)					dbname
,	   count(ec.session_id)						[connection count]
from sys.dm_exec_sessions						es with(nolock)
inner join sys.dm_exec_connections				ec with(nolock)
on es.session_id						=		ec.session_id
group by 
		 ec.client_net_address
,		 es.program_name
,		 es.host_name
,		 es.login_name
,		 es.database_id
order by 
		 ec.client_net_address
,		 es.program_name 

option(recompile);
------
-- This helps you figure where your database load is coming from
-- and verifies connectivity from other machines