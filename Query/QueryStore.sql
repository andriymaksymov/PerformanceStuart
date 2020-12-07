-- Step 1: activate query store (default settings)
use [master];
go

alter database FlowFact set query_store = on;
go

alter database FlowFact set query_store(operation_mode = read_write);
go

-----------------------------------------------------------
-- Step 2: create schema for operations team (logging, monitoring)

use FlowFact
go

if not exists(select 1 from sys.schemas where name = 'ops') 
		exec('create schema ops'); -- Operations / Betrieb
go

if object_id('ops.query_monitor') is null
begin
		create table ops.query_monitor
		(
				query_id				int
		,		query_text_id			int
		,		plan_id					int
		,		query_text				nvarchar(max)
		,		last_exec_time			datetimeoffset
		,		total_wait				int
		,		execution_count			int
		,		avg_duration			float -- (ms)
		,		avg_rowcount			int
		,		avg_physical_io_reads	int
		,		start_time				datetimeoffset
		,		end_time				datetimeoffset
		,		monitor_type			sysname -- 'last executed', 'most used', 'longest execution', 'most expensive', 'most wait'
		,		monitor_time			datetimeoffset	constraint	DF_query_monitor_monitor_time default getutcdate() not null
		);

		create index ix_query_monitor_01 on ops.query_monitor (monitor_type, monitor_time)
end
go

-----------------------------------------------------------
-- Step 3: Monitoring routine

use FlowFact
go

---------------------------------------------------------------------
-- Scan the query store and log the result in ops.query_monitor table for later analysis
-- 1. 25 last executed queries
-- 2. 25 most used queries (most executed)
-- 3. 25 longest duration in last hour
-- 4. 25 most expensiv in IO reads in last 24 hours
-- 5. 25 most waited queries
-- 6. all queries, which execution duration performance dropped in last 48 hour more the twice

-- Andriy Maksymov Consulting Services
-- 10.10.2020	-	created
-- 07.12.2020	-	ausblendung von sys.query_store_wait_stats wegen Version "SQL Server 2017 (14.x) und höher"
---------------------------------------------------------------------
create procedure ops.sp_monitor_querystore
as
begin
	set nocount on

	-- Query Store keeps a history of compilation and runtime metrics throughout query executions, allowing you to ask questions about your workload.
	insert ops.query_monitor
	(
			query_text
	,		query_id
	,		query_text_id
	,		plan_id
	,		last_exec_time
	,		monitor_type
	)
	select top 25 
			S.query_sql_text
	,		J01.query_id
	,		J01.query_text_id
	,		J02.plan_id
	,		J03.last_execution_time
	,		'last executed'
	from	sys.query_store_query_text			S
	join	sys.query_store_query				J01
	on		S.query_text_id				=		J01.query_text_id

	join	sys.query_store_plan				J02
	on		J01.query_id				=		J02.query_id

	join	sys.query_store_runtime_stats		J03
	on		J02.plan_id					=		J03.plan_id

	order by 
			J03.last_execution_time		desc;

	-- Number of executions for each query
	insert ops.query_monitor
	(
			query_id
	,		query_text_id
	,		query_text
	,		execution_count
	,		monitor_type
	)
	select top 25
			J01.query_id
	,		S.query_text_id
	,		S.query_sql_text
	,		sum(J03.count_executions)		total_exec_count
	--,		avg(J03.avg_duration)			avg_duration
	--,		avg(avg_cpu_time)				avg_cpu_time
	,		'most used'

	from	sys.query_store_query_text		S
	join	sys.query_store_query			J01
	on		S.query_text_id			=		J01.query_text_id
	join	sys.query_store_plan			J02
	on		J01.query_id			=		J02.query_id
	join	sys.query_store_runtime_stats	J03
	on		J02.plan_id				=		J03.plan_id
	group by 
			 J01.query_id
	,		 S.query_text_id
	,		 S.query_sql_text
	order by 
			 total_exec_count desc;


	-- The number of queries with the longest average execution time within last hour
	insert ops.query_monitor
	(
			query_id
	,		query_text_id
	,		plan_id
	,		query_text
	,		last_exec_time
	,		avg_duration
	,		monitor_type
	)
	select top 25
			q.query_id
	,		qt.query_text_id
	,		p.plan_id
	,		qt.query_sql_text
	,		rs.last_execution_time
	,		rs.avg_duration
	,		'longest execution'
	from	sys.query_store_query_text as qt
	join	sys.query_store_query as q
	on		qt.query_text_id = q.query_text_id
	join	sys.query_store_plan as p
	on		q.query_id = p.query_id
	join	sys.query_store_runtime_stats as rs
	on		p.plan_id = rs.plan_id
	where
			rs.last_execution_time		>		dateadd(hour, -1, getutcdate())
	order by 
			rs.avg_duration desc;

	-- The number of queries that had the biggest average physical I/O reads in last 24 hours, with corresponding average row count and execution count
	insert ops.query_monitor
	(
			query_id
	,		query_text_id
	,		plan_id
	,		query_text
	,		start_time
	,		end_time
	,		avg_rowcount
	,		execution_count
	,		avg_physical_io_reads
	,		monitor_type
	)
	select top 25 
			q.query_id
	,		qt.query_text_id
	,		p.plan_id
	,		qt.query_sql_text
	--,		rs.runtime_stats_id
	,		rsi.start_time
	,		rsi.end_time
	,		rs.avg_rowcount
	,		rs.count_executions
	,		rs.avg_physical_io_reads
	,		'most expensive'
	from sys.query_store_query_text as qt
	join sys.query_store_query as q
	on qt.query_text_id = q.query_text_id
	join sys.query_store_plan as p
	on q.query_id = p.query_id
	join sys.query_store_runtime_stats as rs
	on p.plan_id = rs.plan_id
	join sys.query_store_runtime_stats_interval as rsi
	on rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
	where 
			rsi.start_time >= dateadd(hour, -24, getutcdate())
	order by 
			rs.avg_physical_io_reads desc;

	---- SQL Server 2017 (14.x) und höher

	--declare @prodVersion int = (select cast(SERVERPROPERTY('ProductMajorVersion') as int))
	--if @prodVersion >= 14
	--	-- This query will return top 10 queries that wait the most
	--	insert ops.query_monitor
	--	(
	--			query_id
	--	,		query_text_id
	--	,		plan_id
	--	,		query_text
	--	,		total_wait
	--	,		monitor_type
	--	)
	--	select top 25 
	--			q.query_id
	--	,		qt.query_text_id
	--	,		p.plan_id
	--	,		max(qt.query_sql_text)
	--	,		sum(total_query_wait_time_ms) as sum_total_wait_ms
	--	,		'most wait'
	--	from sys.query_store_wait_stats ws
	--	join sys.query_store_plan p
	--	on ws.plan_id = p.plan_id
	--	join sys.query_store_query q
	--	on p.query_id = q.query_id
	--	join sys.query_store_query_text qt
	--	on q.query_text_id = qt.query_text_id
	--	group by 
	--			 qt.query_text_id
	--	,		 q.query_id
	--	,		 p.plan_id
	--	order by 
	--			 sum_total_wait_ms desc;

	-- This query will return top 10 queries that wait the most
	insert ops.query_monitor
	(
			query_id
	,		query_text_id
	,		plan_id
	,		query_text
	,		start_time
	,		avg_duration
	,		monitor_type
	)
	select
			q.query_id
	,		qt.query_text_id
	,		p2.plan_id
	,		qt.query_sql_text
	,		rsi2.start_time
	,		rs2.avg_duration
	,		'regress over 50%'

	from sys.query_store_query_text as qt
	join sys.query_store_query as q
	on qt.query_text_id = q.query_text_id
	join sys.query_store_plan as p1
	on q.query_id = p1.query_id
	join sys.query_store_runtime_stats as rs1
	on p1.plan_id = rs1.plan_id
	join sys.query_store_runtime_stats_interval as rsi1
	on rsi1.runtime_stats_interval_id = rs1.runtime_stats_interval_id

	join sys.query_store_plan as p2
	on q.query_id = p2.query_id
	join sys.query_store_runtime_stats as rs2
	on p2.plan_id = rs2.plan_id
	join sys.query_store_runtime_stats_interval as rsi2
	on rsi2.runtime_stats_interval_id = rs2.runtime_stats_interval_id

	where 
			rsi1.start_time > dateadd(hour, -48, getutcdate())
	and		rsi2.start_time > rsi1.start_time
	and		p1.plan_id <> p2.plan_id
	and		rs2.avg_duration > 2 * rs1.avg_duration

	--order by 
	--		q.query_id
	--,		rsi1.start_time
	--,		rsi2.start_time;

end;
-- eop
go

-----------------------------------------------------------
-- Step 4: Create SQL agent job thet calls the ops.sp_monitor_querystore hourly. For example:
exec ops.sp_monitor_querystore 
go

