# openGauss vs PostgreSQL WAL 结构对比文档

本文档详细对比了 openGauss 和 PostgreSQL (Neon 使用的基础版本) 在 WAL (Write-Ahead Logging) 记录结构、常量、CRC 算法、RMGR (Resource Manager) 函数表等方面的差异。

## 1. WAL 记录基本结构对比

### XLogRecord 结构体对比

#### PostgreSQL XLogRecord (vendor/postgres-v17/src/include/access/xlogrecord.h)

```c
typedef struct XLogRecord
{
	uint32		xl_tot_len;		/* total len of entire record */
	TransactionId xl_xid;		/* xact id */
	XLogRecPtr	xl_prev;		/* ptr to previous record in log */
	uint8		xl_info;		/* flag bits, see below */
	RmgrId		xl_rmid;		/* resource manager for this record */
	/* 2 bytes of padding here, initialize to zero */
	pg_crc32c	xl_crc;			/* CRC for this record */

	/* XLogRecordBlockHeaders and XLogRecordDataHeader follow, no padding */
} XLogRecord;
```

#### openGauss XLogRecord (openGauss/src/include/access/xlog_basic.h)

```c
typedef struct XLogRecord
{
	uint32		xl_tot_len;		/* total len of entire record */
	TransactionId xl_xid;		/* xact id */
	XLogRecPtr	xl_prev;		/* ptr to previous record in log */
	uint8		xl_info;		/* flag bits, see below */
	RmgrId		xl_rmid;		/* resource manager for this record */
	uint32		xl_term;		/* **新增字段**: term for consensus */
	uint2		xl_bucket_id;	/* **新增字段**: bucket id for hash bucket */
	/* 0 bytes of padding here, initialize to zero */
	pg_crc32c	xl_crc;			/* CRC for this record */

	/* XLogRecordBlockHeaders and XLogRecordDataHeader follow, no padding */
} XLogRecord;
```

**关键差异**:
- openGauss 添加了 `xl_term` (uint32) 字段用于共识协议
- openGauss 添加了 `xl_bucket_id` (uint2) 字段用于哈希桶表
- PostgreSQL 有 2 字节填充，openGauss 没有填充

### XLogRecordBlockHeader 结构体对比

#### PostgreSQL XLogRecordBlockHeader

```c
typedef struct XLogRecordBlockHeader
{
	uint8		id;				/* block reference ID */
	uint8		fork_flags;		/* fork within the relation, and flags */
	uint16		data_length;	/* number of payload bytes (not including page image) */

	/* If BKPBLOCK_HAS_IMAGE, an XLogRecordBlockImageHeader struct follows */
	/* If BKPBLOCK_SAME_REL is not set, a RelFileLocator follows */
	/* BlockNumber follows */
} XLogRecordBlockHeader;
```

#### openGauss XLogRecordBlockHeader

```c
typedef struct XLogRecordBlockHeader {
    uint8 id;           /* block reference ID */
    uint8 fork_flags;   /* fork within the relation, and flags */
    uint16 data_length; /* number of payload bytes (not including page image) */

    /* If BKPBLOCK_HAS_IMAGE, an XLogRecordBlockImageHeader struct follows */
    /* If !BKPBLOCK_SAME_REL is not set, a RelFileNode follows */
    /* BlockNumber follows */
} XLogRecordBlockHeader;
```

**差异**:
- 注释中 RelFileLocator vs RelFileNode 的差异反映了文件定位器的不同实现

## 2. WAL 记录标志位对比

### xl_info 标志位

#### PostgreSQL 标志位 (vendor/postgres-v17/src/include/access/xlogrecord.h)

```c
#define XLR_INFO_MASK			0x0F
#define XLR_RMGR_INFO_MASK		0xF0

#define XLR_SPECIAL_REL_UPDATE	0x01
#define XLR_CHECK_CONSISTENCY	0x02
```

#### openGauss 标志位 (openGauss/src/include/access/xlogrecord.h)

```c
#define XLR_INFO_MASK 0x0F
#define XLR_RMGR_INFO_MASK 0xF0

#define XLR_SPECIAL_REL_UPDATE 0x01
#define XLR_BTREE_UPGRADE_FLAG 0x02    /* **新增**: BTREE 升级标志 */
#define XLR_REL_COMPRESS       0X04    /* **新增**: 压缩表创建 */
#define XLR_IS_TOAST           0X08    /* **新增**: TOAST 页面 */

#define XLR_CHECK_CONSISTENCY   0x03   /* **不同值**: 一致性检查 */
```

**关键差异**:
- openGauss 添加了 BTREE_UPGRADE_FLAG、REL_COMPRESS、IS_TOAST 标志
- CHECK_CONSISTENCY 的值不同 (0x02 vs 0x03)

### Block Header 标志位

#### PostgreSQL fork_flags 标志位

```c
#define BKPBLOCK_FORK_MASK	0x0F
#define BKPBLOCK_FLAG_MASK	0xF0
#define BKPBLOCK_HAS_IMAGE	0x10
#define BKPBLOCK_HAS_DATA	0x20
#define BKPBLOCK_WILL_INIT	0x40
```

#### openGauss fork_flags 标志位

```c
#define BKPBLOCK_FORK_MASK	0x0F
#define BKPBLOCK_FLAG_MASK	0xF0
#define BKPBLOCK_HAS_IMAGE	0x10
#define BKPBLOCK_HAS_DATA	0x20
#define BKPBLOCK_WILL_INIT	0x40
#define BKID_HAS_BUCKET_OR_SEGPAGE (0x80)  /* **新增**: 哈希桶或段页面 */
#define BKID_GET_BKID(id) (id & 0x3F)      /* **新增**: 获取块ID的宏 */
```

**差异**:
- openGauss 添加了哈希桶和段页面支持的标志位

## 3. RMGR (Resource Manager) 对比

### RMGR ID 常量

#### PostgreSQL RMGR IDs (vendor/postgres-v17/src/include/access/rmgrlist.h)

```c
PG_RMGR(RM_XLOG_ID, "XLOG", xlog_redo, xlog_desc, xlog_identify, NULL, NULL, NULL, xlog_decode)
PG_RMGR(RM_XACT_ID, "Transaction", xact_redo, xact_desc, xact_identify, NULL, NULL, NULL, xact_decode)
PG_RMGR(RM_SMGR_ID, "Storage", smgr_redo, smgr_desc, smgr_identify, NULL, NULL, NULL, NULL)
PG_RMGR(RM_CLOG_ID, "CLOG", clog_redo, clog_desc, clog_identify, NULL, NULL, NULL, NULL)
PG_RMGR(RM_DBASE_ID, "Database", dbase_redo, dbase_desc, dbase_identify, NULL, NULL, NULL, NULL)
PG_RMGR(RM_TBLSPC_ID, "Tablespace", tblspc_redo, tblspc_desc, tblspc_identify, NULL, NULL, NULL, NULL)
PG_RMGR(RM_MULTIXACT_ID, "MultiXact", multixact_redo, multixact_desc, multixact_identify, NULL, NULL, NULL, NULL)
PG_RMGR(RM_RELMAP_ID, "RelMap", relmap_redo, relmap_desc, relmap_identify, NULL, NULL, NULL, NULL)
PG_RMGR(RM_STANDBY_ID, "Standby", standby_redo, standby_desc, standby_identify, NULL, NULL, NULL, standby_decode)
PG_RMGR(RM_HEAP2_ID, "Heap2", heap2_redo, heap2_desc, heap2_identify, NULL, NULL, heap_mask, heap2_decode)
PG_RMGR(RM_HEAP_ID, "Heap", heap_redo, heap_desc, heap_identify, NULL, NULL, heap_mask, heap_decode)
PG_RMGR(RM_BTREE_ID, "Btree", btree_redo, btree_desc, btree_identify, btree_xlog_startup, btree_xlog_cleanup, btree_mask, NULL)
PG_RMGR(RM_HASH_ID, "Hash", hash_redo, hash_desc, hash_identify, NULL, NULL, hash_mask, NULL)
PG_RMGR(RM_GIN_ID, "Gin", gin_redo, gin_desc, gin_identify, gin_xlog_startup, gin_xlog_cleanup, gin_mask, NULL)
PG_RMGR(RM_GIST_ID, "Gist", gist_redo, gist_desc, gist_identify, gist_xlog_startup, gist_xlog_cleanup, gist_mask, NULL)
PG_RMGR(RM_SEQ_ID, "Sequence", seq_redo, seq_desc, seq_identify, NULL, NULL, seq_mask, NULL)
PG_RMGR(RM_SPGIST_ID, "SPGist", spg_redo, spg_desc, spg_identify, spg_xlog_startup, spg_xlog_cleanup, spg_mask, NULL)
PG_RMGR(RM_BRIN_ID, "BRIN", brin_redo, brin_desc, brin_identify, NULL, NULL, brin_mask, NULL)
PG_RMGR(RM_COMMIT_TS_ID, "CommitTs", commit_ts_redo, commit_ts_desc, commit_ts_identify, NULL, NULL, NULL, NULL)
PG_RMGR(RM_REPLORIGIN_ID, "ReplicationOrigin", replorigin_redo, replorigin_desc, replorigin_identify, NULL, NULL, NULL, NULL)
PG_RMGR(RM_GENERIC_ID, "Generic", generic_redo, generic_desc, generic_identify, NULL, NULL, generic_mask, NULL)
PG_RMGR(RM_LOGICALMSG_ID, "LogicalMessage", logicalmsg_redo, logicalmsg_desc, logicalmsg_identify, NULL, NULL, NULL, logicalmsg_decode)
```

#### PostgreSQL RMGR 数量: 22 (ID 0-21)

#### openGauss RMGR IDs (openGauss/src/include/access/rmgrlist.h)

```c
PG_RMGR(RM_XLOG_ID, "XLOG", xlog_redo, xlog_desc, NULL, NULL, NULL, NULL, NULL, xlog_type_name)
PG_RMGR(RM_XACT_ID, "Transaction", xact_redo, xact_desc, NULL, NULL, NULL, NULL, NULL, xact_type_name)
PG_RMGR(RM_SMGR_ID, "Storage", smgr_redo, smgr_desc, NULL, NULL, NULL, NULL, NULL, smgr_type_name)
PG_RMGR(RM_CLOG_ID, "CLOG", clog_redo, clog_desc, NULL, NULL, NULL, NULL, NULL, clog_type_name)
PG_RMGR(RM_DBASE_ID, "Database", dbase_redo, dbase_desc, NULL, NULL, NULL, NULL, NULL, dbase_type_name)
PG_RMGR(RM_TBLSPC_ID, "Tablespace", tblspc_redo, tblspc_desc, NULL, NULL, NULL, NULL, NULL, tblspc_type_name)
PG_RMGR(RM_MULTIXACT_ID, "MultiXact", multixact_redo, multixact_desc, NULL, NULL, NULL, NULL, NULL, multixact_type_name)
PG_RMGR(RM_RELMAP_ID, "RelMap", relmap_redo, relmap_desc, NULL, NULL, NULL, NULL, NULL, relmap_type_name)
PG_RMGR(RM_STANDBY_ID, "Standby", standby_redo, standby_desc, StandbyXlogStartup, StandbyXlogCleanup, NULL, NULL, NULL, standby_type_name)
PG_RMGR(RM_HEAP2_ID, "Heap2", heap2_redo, heap2_desc, NULL, NULL, NULL, NULL, NULL, heap2_type_name)
PG_RMGR(RM_HEAP_ID, "Heap", heap_redo, heap_desc, NULL, NULL, NULL, NULL, NULL, heap_type_name)
PG_RMGR(RM_BTREE_ID, "Btree", btree_redo, btree_desc, btree_xlog_startup, btree_xlog_cleanup, btree_safe_restartpoint, NULL, NULL, btree_type_name)
PG_RMGR(RM_HASH_ID, "Hash", hash_redo, hash_desc, NULL, NULL, NULL, NULL, NULL, hash_type_name)
PG_RMGR(RM_GIN_ID, "Gin", gin_redo, gin_desc, gin_xlog_startup, gin_xlog_cleanup, NULL, NULL, NULL, gin_type_name)
PG_RMGR(RM_GIST_ID, "Gist", gist_redo, gist_desc, gist_xlog_startup, gist_xlog_cleanup, NULL, NULL, NULL, gist_type_name)
PG_RMGR(RM_SEQ_ID, "Sequence", seq_redo, seq_desc, NULL, NULL, NULL, NULL, NULL, seq_type_name)
PG_RMGR(RM_SPGIST_ID, "SPGist", spg_redo, spg_desc, spg_xlog_startup, spg_xlog_cleanup, NULL, NULL, NULL, spg_type_name)
PG_RMGR(RM_SLOT_ID, "Slot", slot_redo, slot_desc, NULL, NULL, NULL, NULL, NULL, slot_type_name)
PG_RMGR(RM_HEAP3_ID, "Heap3", heap3_redo, heap3_desc, NULL, NULL, NULL, NULL, NULL, heap3_type_name)
PG_RMGR(RM_BARRIER_ID, "Barrier", barrier_redo, barrier_desc, NULL, NULL, NULL, NULL, NULL, barrier_type_name)

#ifdef ENABLE_MOT
PG_RMGR(RM_MOT_ID, "MOT", MOTRedo, MOTDesc, NULL, NULL, NULL, NULL, NULL, MOT_type_name)
#endif

PG_RMGR(RM_UHEAP_ID, "UHeap", UHeapRedo, UHeapDesc, NULL, NULL, NULL, UHeapUndoActions, NULL, uheap_type_name)
PG_RMGR(RM_UHEAP2_ID, "UHeap2", UHeap2Redo, UHeap2Desc, NULL, NULL, NULL, NULL, NULL, uheap2_type_name)
PG_RMGR(RM_UNDOLOG_ID, "UndoLog", undo::UndoXlogRedo, undo::UndoXlogDesc, NULL, NULL, NULL, NULL, NULL, undo::undo_xlog_type_name)
PG_RMGR(RM_UHEAPUNDO_ID, "UHeapUndo", UHeapUndoRedo, UHeapUndoDesc, NULL, NULL, NULL, NULL, NULL, uheap_undo_type_name)
PG_RMGR(RM_UNDOACTION_ID, "UndoAction", undo::UndoXlogRollbackFinishRedo, undo::UndoXlogRollbackFinishDesc, NULL, NULL, NULL, NULL, NULL, undo::undo_xlog_roll_back_finish_type_name)
PG_RMGR(RM_UBTREE_ID, "UBtree", UBTreeRedo, UBTreeDesc, UBTreeXlogStartup, UBTreeXlogCleanup, UBTreeSafeRestartPoint, NULL, NULL, ubtree_type_name)
PG_RMGR(RM_UBTREE2_ID, "UBtree2", UBTree2Redo, UBTree2Desc, NULL, NULL, NULL, NULL, NULL, ubtree2_type_name)
PG_RMGR(RM_SEGPAGE_ID, "SegpageStorage", segpage_smgr_redo, segpage_smgr_desc, NULL, NULL, NULL, NULL, NULL, segpage_smgr_type_name)
PG_RMGR(RM_REPLORIGIN_ID, "ReplicationOrigin", replorigin_redo, replorigin_desc, NULL, NULL, NULL, NULL, NULL, replorigin_type_name)
PG_RMGR(RM_COMPRESSION_REL_ID, "CompressionRelation", CfsShrinkRedo, CfsShrinkDesc, NULL, NULL, NULL, NULL, NULL, CfsShrinkTypeName)
PG_RMGR(RM_LOGICALDDLMSG_ID, "LogicalDDLMessage", logicalddlmsg_redo, logicalddlmsg_desc, NULL, NULL, NULL, NULL, NULL, logicalddlmsg_type_name)
PG_RMGR(RM_GENERIC_ID, "Generic", generic_redo, generic_desc, NULL, NULL, NULL, NULL, NULL, NULL)
PG_RMGR(RM_UBTREE3_ID, "UBtree3", UBTree3Redo, UBTree3Desc, NULL, NULL, NULL, UBTreePCRRollback, NULL, ubtree3_type_name)
PG_RMGR(RM_UBTREE4_ID, "UBtree4", UBTree4Redo, UBTree4Desc, NULL, NULL, NULL, NULL, NULL, ubtree4_type_name)
```

openGauss RMGR 数量: 35+ (ID 0-34+)

**openGauss 新增的 RMGR**:
- RM_SLOT_ID (24): Slot 管理
- RM_HEAP3_ID (25): Heap3 操作
- RM_BARRIER_ID (26): Barrier 操作
- RM_MOT_ID (27): Memory-Optimized Table (条件编译)
- RM_UHEAP_ID (28): UHeap (Unified Heap)
- RM_UHEAP2_ID (29): UHeap2
- RM_UNDOLOG_ID (30): Undo Log
- RM_UHEAPUNDO_ID (31): UHeap Undo
- RM_UNDOACTION_ID (32): Undo Action
- RM_UBTREE_ID (33): UHeap BTree
- RM_UBTREE2_ID (34): UHeap BTree2
- RM_SEGPAGE_ID (35): Segment Page Storage
- RM_COMPRESSION_REL_ID (37): Compression Relation
- RM_LOGICALDDLMSG_ID (38): Logical DDL Message
- RM_UBTREE3_ID (40): UHeap BTree3
- RM_UBTREE4_ID (41): UHeap BTree4

## 4. 具体 WAL 记录类型对比

### Heap 操作记录类型

#### PostgreSQL Heap WAL 记录类型 (vendor/postgres-v17/src/include/access/heapam_xlog.h)

```c
#define XLOG_HEAP_INSERT		0x00
#define XLOG_HEAP_DELETE		0x10
#define XLOG_HEAP_UPDATE		0x20
#define XLOG_HEAP_TRUNCATE		0x30
#define XLOG_HEAP_HOT_UPDATE	0x40
#define XLOG_HEAP_CONFIRM		0x50
#define XLOG_HEAP_LOCK			0x60
#define XLOG_HEAP_INPLACE		0x70

#define XLOG_HEAP_OPMASK		0x70
#define XLOG_HEAP_INIT_PAGE		0x80

#define XLOG_HEAP2_REWRITE		0x00
#define XLOG_HEAP2_PRUNE_ON_ACCESS		0x10
#define XLOG_HEAP2_PRUNE_VACUUM_SCAN	0x20
#define XLOG_HEAP2_PRUNE_VACUUM_CLEANUP	0x30
#define XLOG_HEAP2_VISIBLE		0x40
#define XLOG_HEAP2_MULTI_INSERT 0x50
#define XLOG_HEAP2_LOCK_UPDATED 0x60
```

#### openGauss Heap WAL 记录类型 (openGauss/src/include/access/htup.h)

```c
#define XLOG_HEAP_INSERT 0x00
#define XLOG_HEAP_DELETE 0x10
#define XLOG_HEAP_UPDATE 0x20
#define XLOG_HEAP_BASE_SHIFT 0x30    /* **重命名**: 原 TRUNCATE */
#define XLOG_HEAP_HOT_UPDATE 0x40
#define XLOG_HEAP_NEWPAGE 0x50        /* **重命名**: 原 CONFIRM */
#define XLOG_HEAP_LOCK 0x60
#define XLOG_HEAP_INPLACE 0x70

#define XLOG_HEAP_OPMASK 0x70
#define XLOG_HEAP_INIT_PAGE 0x80

#define XLOG_HEAP2_FREEZE 0x00        /* **不同**: 原 REWRITE */
#define XLOG_HEAP2_CLEAN 0x10          /* **不同**: 原 PRUNE_ON_ACCESS */
#define XLOG_HEAP2_PAGE_UPGRADE 0x20   /* **不同**: 原 PRUNE_VACUUM_SCAN */
#define XLOG_HEAP2_CLEANUP_INFO 0x30   /* **不同**: 原 PRUNE_VACUUM_CLEANUP */
#define XLOG_HEAP2_VISIBLE 0x40
#define XLOG_HEAP2_MULTI_INSERT 0x50
#define XLOG_HEAP2_BCM 0x60            /* **不同**: 原 LOCK_UPDATED */
```

**关键差异**:
- 部分操作码值相同，但语义可能不同
- openGauss 有独特的操作如 BCM (Block Change Map)

## 5. CRC 算法对比

### PostgreSQL CRC 算法 (vendor/postgres-v17/src/include/utils/pg_crc.h)

```c
/*
 * The CRC algorithm used for WAL et al in pre-9.5 versions.
 *
 * This closely resembles the normal CRC-32 algorithm, but is subtly
 * different. Using Williams' terms, we use the "normal" table, but with
 * "reflected" code. That's bogus, but it was like that for years before
 * anyone noticed.
 */
#define INIT_LEGACY_CRC32(crc) ((crc) = 0xFFFFFFFF)
#define FIN_LEGACY_CRC32(crc)	((crc) ^= 0xFFFFFFFF)
#define COMP_LEGACY_CRC32(crc, data, len)	\
	COMP_CRC32_REFLECTED_TABLE(crc, data, len, pg_crc32_table)
#define EQ_LEGACY_CRC32(c1, c2) ((c1) == (c2))

#define COMP_CRC32_REFLECTED_TABLE(crc, data, len, table) \
do {															  \
	const unsigned char *__data = (const unsigned char *) (data); \
	uint32		__len = (len); \
\
	while (__len-- > 0) \
	{ \
		int		__tab_index = ((int) ((crc) >> 24) ^ *__data++) & 0xFF; \
		(crc) = table[__tab_index] ^ ((crc) << 8); \
	} \
} while (0)
```

### openGauss CRC 算法 (openGauss/src/include/utils/pg_crc.h)

```c
/*
 * The CRC algorithm used for WAL et al in pre-9.5 versions.
 *
 * This closely resembles the normal CRC-32 algorithm, but is subtly
 * different. Using Williams' terms, we use the "normal" table, but with
 * "reflected" code. That's bogus, but it was like that for years before
 * anyone noticed. It does not correspond to any polynomial in a normal CRC
 * algorithm, so it's not clear what the error-detection properties of this
 * algorithm actually are.
 *
 * We still need to carry this around because it is used in a few on-disk
 * structures that need to be pg_upgradeable. It should not be used in new
 * code.
 *
 * Deprecated
 *
 * using CRC32C instead
 */
#define INIT_TRADITIONAL_CRC32(crc) ((crc) = 0xFFFFFFFF)
#define FIN_TRADITIONAL_CRC32(crc)  ((crc) ^= 0xFFFFFFFF)
#define INIT_CRC32(crc) ((crc) = 0xFFFFFFFF)
#define FIN_CRC32(crc) ((crc) ^= 0xFFFFFFFF)

#define COMP_CRC32(crc, data, len)                                     \
    do {                                                               \
        const unsigned char* __data = (const unsigned char*)(data);    \
        uint32 __len = (len);                                          \
                                                                       \
        while (__len-- > 0) {                                          \
            int __tab_index = ((int)((crc) >> 24) ^ *__data++) & 0xFF; \
            (crc) = pg_crc32_table[__tab_index] ^ ((crc) << 8);        \
        }                                                              \
    } while (0)

#define COMP_TRADITIONAL_CRC32(crc, data, len)	\
	COMP_CRC32_NORMAL_TABLE(crc, data, len, pg_crc32_table)
```

**差异**:
- PostgreSQL 区分 LEGACY_CRC32 和 TRADITIONAL_CRC32
- openGauss 主要使用 COMP_CRC32 (与 PostgreSQL 的 LEGACY_CRC32 相同)
- 两者都使用相同的 CRC32 表和算法，但命名和宏定义略有不同

## 6. 页面头结构对比

### PostgreSQL 页面头 (vendor/postgres-v17/src/include/storage/bufpage.h)

```c
typedef struct PageHeaderData
{
	/* XXX LSN is member of *any* block, not only page-organized ones */
	PageXLogRecPtr pd_lsn;		/* LSN: next byte after last byte of xlog
								 * record for last change to this page */
	uint16		pd_checksum;	/* checksum */
	uint16		pd_flags;		/* flag bits, see below */
	LocationIndex pd_lower;		/* offset to start of free space */
	LocationIndex pd_upper;		/* offset to end of free space */
	LocationIndex pd_special;	/* offset to start of special space */
	uint16		pd_pagesize_version;
	uint32		pd_prune_xid;	/* oldest prunable XID, or zero if none */
	ItemIdData	pd_linp[];		/* line pointer array */
} PageHeaderData;
```

### openGauss 页面头 (需要进一步调查 - 在相关头文件中)

openGauss 的页面头结构可能包含额外的字段用于段页面存储和哈希桶支持。

## 7. 计算节点启动时的 Bootstrap 表

计算节点启动时需要读取以下关键的 bootstrap 表来初始化系统目录：

### 核心 Bootstrap 表

1. **pg_class** - 关系定义表
   - 包含所有表、索引、视图等的元数据
   - 字段：oid, relname, relnamespace, reltype, reloftype, relowner, relam, relfilenode, reltablespace, relpages, reltuples, relallvisible, reltoastrelid, reltoastidxid, reldispatcher, relbucket, relnatts, relchecks, relhasrules, relhastriggers, relhassubclass, relrowsecurity, relforcerowsecurity, relispopulated, relreplident, relispartition, relfrozenxid, relminmxid, relacl, reloptions, relpartbound

2. **pg_attribute** - 属性定义表
   - 包含所有列的定义
   - 字段：oid, attrelid, attname, atttypid, attstattarget, attlen, attnum, attndims, attcacheoff, atttypmod, attbyval, attstorage, attalign, attnotnull, atthasdef, atthasmissing, attidentity, attgenerated, attisdropped, attislocal, attinhcount, attcollation, attacl, attoptions, attfdwoptions, attmissingval

3. **pg_type** - 类型定义表
   - 包含所有数据类型的定义

4. **pg_namespace** - 命名空间定义表
   - 包含 schema 的定义

5. **pg_proc** - 函数定义表
   - 包含所有函数和操作符的定义

6. **pg_operator** - 操作符定义表

7. **pg_opclass** - 操作符类定义表

8. **pg_am** - 访问方法定义表

9. **pg_index** - 索引定义表

10. **pg_constraint** - 约束定义表

### Bootstrap 过程关键点

- 这些表在计算节点启动时必须从 WAL 或快照中恢复
- openGauss 可能有额外的系统表用于段页面存储和 UHeap 支持
- 计算节点需要能够解析这些表的 WAL 记录来进行正确的重放

## 8. Neon 适配 openGauss WAL 的实现建议

### 1. 结构映射层

在 `libs/wal_decoder/src/decoder.rs` 中添加 openGauss 格式支持：

```rust
enum WalFormat {
    Postgres,
    OpenGauss,
}

struct OpenGaussXLogRecord {
    // 映射字段到 Postgres 结构
    xl_tot_len: u32,
    xl_xid: TransactionId,
    xl_prev: XLogRecPtr,
    xl_info: u8,
    xl_rmid: RmgrId,
    xl_term: u32,        // openGauss 特有
    xl_bucket_id: u16,   // openGauss 特有
    xl_crc: pg_crc32c,
}
```

### 2. RMGR 扩展

扩展 RMGR dispatch 表以支持 openGauss 的额外 RMGR：

```rust
match (wal_format, decoded.xl_rmid) {
    (WalFormat::OpenGauss, RM_UHEAP_ID) => Self::decode_uheap_record(...),
    (WalFormat::OpenGauss, RM_UBTREE_ID) => Self::decode_ubtree_record(...),
    // ... 其他 openGauss 特有的 RMGR
    _ => Self::decode_postgres_record(...),
}
```

### 3. CRC 计算适配

根据 WAL 格式选择合适的 CRC 算法：

```rust
fn compute_crc(wal_format: WalFormat, data: &[u8]) -> u32 {
    match wal_format {
        WalFormat::Postgres => compute_legacy_crc32(data),
        WalFormat::OpenGauss => compute_traditional_crc32(data),
    }
}
```

### 4. 标志位映射

创建标志位映射函数来处理不同格式的 xl_info 字段。

## 9. 总结

openGauss 的 WAL 格式与 PostgreSQL 有显著差异：

1. **结构差异**: 添加了 xl_term 和 xl_bucket_id 字段
2. **RMGR 扩展**: 从 22 个增加到 35+ 个 RMGR
3. **标志位变化**: 添加了压缩、TOAST、哈希桶相关的标志
4. **操作码差异**: Heap 操作的部分语义不同
5. **CRC 算法**: 基本相同但宏定义不同

Neon 要适配 openGauss 需要实现一个翻译层，将 openGauss 特有的结构和操作映射到现有的解码逻辑中。</content>
<parameter name="filePath">/Users/heoohuan/neon/docs/opengauss-neon-wal-detailed-diffs.md