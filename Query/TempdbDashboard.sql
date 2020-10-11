
select top 10 
		total_logical_reads / execution_count		avg_logical_reads
,		total_logical_writes / execution_count		avg_logical_writes
,		total_physical_reads / execution_count		avg_phys_reads
,		total_worker_time / execution_count			avg_worker_time
,		total_spills / execution_count / 128		avg_spills_mb

,		last_execution_time
,		execution_count
--,		statement_start_offset						stmt_start_offset
,		(
				select 
						substring
						(
								text
						,		statement_start_offset / 2 + 1
						,		(
										case
												when statement_end_offset = -1
												then len(convert(nvarchar(max), text)) * 2

												else statement_end_offset
										end - statement_start_offset
								) / 2
						)
				from	sys.dm_exec_sql_text(sql_handle)
		)                                            query_text
--,		plan_handle
from	sys.dm_exec_query_stats
where
		total_spills	>	0
order by 
		 total_logical_reads + total_logical_writes desc;


USE tempdb
GO

--Page Free Space (PFS): Tracks the allocation status of each page and
--approximately how much free space it has. There is one PFS page for
--every 1/2 GB of data file. The first PFS page is page number 1 of the
--data file. The second is page 8088 and then it repeats every 8088 pages
--thereafter.
--Global Allocation Map (GAM): Tracks which extents have been allocated.
--There is one GAM page for every 4 GB of data file. The first GAM page is
--page number 2 in the data file, the second is page number 511232, and
--then it repeats every 511,232 pages.
--Shared Global Allocation Map (SGAM): Tracks which extents are being
--used as mixed (shared) extents. There is one SGAM page for every 4
--GB of data file. The first SGAM page is page number 3 in the data file,
--the second is page number 511233, and then it repeats every 511,232
--pages.

--The formula for determining the type of page experiencing contention is:
--PFS: Page ID = 1 or Page ID % 8088
--GAM: Page ID = 2 or Page ID % 511232
--SGAM: Page ID = 3 or (Page ID – 1) % 511232

WITH Tasks AS 
(
	SELECT 
		session_id,
		wait_type,
		wait_duration_ms,
		blocking_session_id,
		resource_description,
		CAST(RIGHT(resource_description, LEN(resource_description) - CHARINDEX(':', resource_description, 3)) AS INT) AS PageID
	FROM sys.dm_os_waiting_tasks
	WHERE wait_type LIKE 'PAGE%LATCH_%'
		AND resource_description LIKE '2:%'
)
SELECT 
	session_id,
	wait_type,
	wait_duration_ms,
	blocking_session_id,
	resource_description,
	CASE
		WHEN PageID = 1 Or PageID % 8088 = 0 THEN 'Is PFS Page'
		WHEN PageID = 2 Or PageID % 511232 = 0 THEN 'Is GAM Page'
		WHEN PageID = 3 Or (PageID - 1) % 511232 = 0 THEN 'Is SGAM Page'
		ELSE 'Is Not PFS, GAM, or SGAM page'
	END AS ResourceType
FROM Tasks
GO

-- Microsoft officially recommends one data file per logical CPU (1:1 ratio)
