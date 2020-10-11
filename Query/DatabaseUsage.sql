-- Breaks down buffers used by current database by object (table, index) in the buffer cache  (Query 67) (Buffer Usage)
-- Note: This query could take some time on a busy instance

select 
	   schema_name(o.Schema_ID) as                 [Schema Name]
,	   object_name(p.object_id) as                 [Object Name]
--,	   p.index_id
,		i.name								   [Index Name]
,	   cast(count(*) / 128.0 as decimal(10, 2)) as [Buffer size(MB)]
,	   count(*) as                                 BufferCount
,	   p.Rows as                                   [Row Count]
,	   p.data_compression_desc as                  [Compression Type]
from sys.allocation_units as a with(nolock)
join sys.dm_os_buffer_descriptors as b with(nolock)
on a.allocation_unit_id = b.allocation_unit_id
join sys.partitions as p with(nolock)
on a.container_id = p.hobt_id
join sys.objects as o with(nolock)
on p.object_id = o.object_id
join	sys.indexes			i
on		o.object_id	=		i.object_id
and		p.index_id	=		i.index_id
where b.database_id = convert(int, db_id())
	  and p.object_id > 100
	  and object_name(p.object_id) not like N'plan_%'
	  and object_name(p.object_id) not like N'sys%'
	  and object_name(p.object_id) not like N'xml_index_nodes%'
group by 
		 o.Schema_ID
,		 p.object_id
,		 i.name
,		 p.data_compression_desc
,		 p.Rows
order by 
		 BufferCount desc option(
								 recompile);
------
-- Tells you what tables and indexes are using the most memory in the buffer cache
-- It can help identify possible candidates for data compression