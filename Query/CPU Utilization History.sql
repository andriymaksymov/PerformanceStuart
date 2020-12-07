-- Get CPU Utilization History for last 256 minutes (in one minute intervals)  (Query 42) (CPU Utilization History)

declare 
	   @ts_now bigint =
(
		select 
				cpu_ticks / (cpu_ticks / ms_ticks)
		from	sys.dm_os_sys_info with(nolock)
);

select top (256) 
	   SQLProcessUtilization								[SQL Server Process CPU Utilization]
,	   SystemIdle											[System Idle Process]
,	   100 - SystemIdle - SQLProcessUtilization				[Other Process CPU Utilization]
,	   dateadd(ms, -1 * (@ts_now - timestamp), getdate())	[Event Time]
from
(
		select 
				record.value('(./Record/@id)[1]', 'int')														record_id
		,		record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int')				SystemIdle
		,		record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int')		SQLProcessUtilization
		,		timestamp
		from
		(
				select 
						timestamp
				,		convert(xml, record)		record
				from	sys.dm_os_ring_buffers with(nolock)
				where 
						ring_buffer_type	=		N'RING_BUFFER_SCHEDULER_MONITOR'
				and		record				like	N'%<SystemHealth>%'
		)		x
)		y
order by 
		 record_id desc
		 
option(recompile);
------