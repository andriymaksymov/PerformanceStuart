-----------------------------------------------------------
-- Step 1: create schema for operations team (logging, monitoring)

use FlowFact
go

if not exists(select 1 from sys.schemas where name = 'ops') 
		exec('create schema ops'); -- Operations / Betrieb
go

drop table ops.session_stats
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
		,		start_time				datetime not null
		,		end_time				datetime
		,		cpu_time				int			-- ms
		,		row_count				bigint		
		,		logical_reads			bigint
		,		reads					bigint
		,		writes					bigint
		,		status					nvarchar(30)  
		,		statement_sql_handle	varbinary(64)
		,		statement_context_id	bigint
		,		statement_text			nvarchar(max)
		,		monitor_type			sysname -- 'session / request'
		,		monitor_time			datetimeoffset	constraint	DF_session_stats_monitor_time default getutcdate() not null
		);

		create index ix_session_stats_01 on ops.session_stats (monitor_type, monitor_time)
end
go


-----------------------------------------------------------
-- Step 2: Monitoring routine

---------------------------------------------------------------------
-- Scan the DMV and log the result in ops.session_stats table for later analysis
-- Andriy Maksymov Consulting Services
-- 07.12.2020	-	created
-- 30.12.2020	-	extended
---------------------------------------------------------------------
alter procedure ops.sp_monitor_session_stats
as
begin
	set nocount on

	insert	ops.session_stats
	(
			client_net_address     
	,		program_name           
	,		host_name              
	,		login_name             
	,		db_name                
	,		start_time
	,		cpu_time
	,		row_count
	,		logical_reads
	,		reads
	,		writes
	,		status
	,		monitor_type
	)

	select 
			J01.client_net_address
	,		S.program_name
	,		S.host_name
	,		S.login_name
	,		db_name(S.database_id)				db_name
	,		S.login_time
	,		S.cpu_time
	,		S.row_count
	,		S.logical_reads
	,		S.reads
	,		S.writes
	,		S.status
	,		'session'
	from	sys.dm_exec_sessions				S
	join	sys.dm_exec_connections				J01
	on		S.session_id				=		J01.session_id

	--where
	--		J01.client_net_address		not	in	('<local machine>', '::1')


	-- request
	insert	ops.session_stats
	(
			client_net_address     
	,		program_name           
	,		host_name              
	,		login_name             
	,		db_name                
	,		start_time
	,		end_time
	,		cpu_time
	,		row_count
	,		logical_reads
	,		reads
	,		writes
	,		status
	,		statement_sql_handle
	,		statement_context_id
	,		statement_text
	,		monitor_type
	)

	select 
			J01.client_net_address
	,		S.program_name
	,		S.host_name
	,		S.login_name
	,		db_name(S.database_id)						db_name
	,		S.last_request_start_time
	,		S.last_request_end_time
	,		J02.cpu_time
	,		J02.row_count
	,		J02.logical_reads
	,		J02.reads
	,		J02.writes
	,		J02.status
	,		J02.statement_sql_handle
	,		J02.statement_context_id
	,		iif
			(
					J03.objectid is null
			,		J03.text
			,		N'exec ' + quotename(db_name(st.dbid)) + N'.' + quotename(object_schema_name(st.objectid, st.dbid)) + N'.' + quotename(object_name(st.objectid, st.dbid)) -- It will display the Stored Procedure's Name.
			)
	,		'request'
	from	sys.dm_exec_sessions						S
	join	sys.dm_exec_connections						J01
	on		S.session_id						=		J01.session_id

	join	sys.dm_exec_requests						J02
	on		S.session_id						=		J02.session_id

	cross apply sys.dm_exec_sql_text(J02.sql_handle)	J03

	--where
	--		J01.client_net_address		not	in	('<local machine>', '::1')

end;
-- eop
go

-----------------------------------------------------------
-- Step 3: Create/Expand SQL agent job that calls the ops.sp_monitor_session_stats hourly. For example:
exec ops.sp_monitor_session_stats 
go

