-----------------------------------------------------------
-- Step 1: create schema for operations team (logging, monitoring)

use FlowFact
go

if not exists(select 1 from sys.schemas where name = 'ops') 
		exec('create schema ops'); -- Operations / Betrieb
go

if object_id('ops.session_stats') is null
begin
		create table ops.session_stats
		(
				client_net_address      nvarchar(48) 
		,		program_name            nvarchar(128) 
		,		host_name               nvarchar(128) 
		,		login_name              nvarchar(128) 
		,		db_name                 nvarchar(128) 
		,		last_request_start_time datetime not null
		,		monitor_type			sysname -- 'session'
		,		monitor_time			datetimeoffset	constraint	DF_session_stats_monitor_time default getutcdate() not null
		);

		create index ix_session_stats_01 on ops.session_stats (monitor_type, monitor_time)
end
go


-----------------------------------------------------------
-- Step 2: Monitoring routine

use FlowFact
go

---------------------------------------------------------------------
-- Scan the DMV and log the result in ops.session_stats table for later analysis
-- 1. 25 last executed queries


-- Andriy Maksymov Consulting Services
-- 07.12.2020	-	created
---------------------------------------------------------------------
create procedure ops.sp_monitor_session_stats
as
begin
	set nocount on

	-- Get a count of SQL connections by IP address (Query 39) (Connection Counts by IP Address)
	insert	ops.session_stats
	(
			client_net_address     
	,		program_name           
	,		host_name              
	,		login_name             
	,		db_name                
	,		last_request_start_time
	,		monitor_type
	)

	select 
			J01.client_net_address
	,		S.program_name
	,		S.host_name
	,		S.login_name
	,		db_name(S.database_id)				db_name
	,		S.last_request_start_time
	,		'session'
	from	sys.dm_exec_sessions				S
	join	sys.dm_exec_connections				J01
	on		S.session_id				=		J01.session_id

	--where
	--		J01.client_net_address		not	in	('<local machine>', '::1')

end;
-- eop
go

-----------------------------------------------------------
-- Step 3: Create/Expand SQL agent job that calls the ops.sp_monitor_session_stats hourly. For example:
exec ops.sp_monitor_session_stats 
go

