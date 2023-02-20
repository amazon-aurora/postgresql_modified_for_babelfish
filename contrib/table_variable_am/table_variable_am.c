/*-------------------------------------------------------------------------
 *
 * table_variable_am.c
 *		Index AM template main file.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/table_variable_am
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "tv_heapam.h"

#include "access/amapi.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/heaptoast.h"
#include "access/multixact.h"
#include "access/rewriteheap.h"
#include "access/syncscan.h"
#include "access/tableam.h"
#include "access/tsmapi.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/progress.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

void		_PG_init(void);
PG_FUNCTION_INFO_V1(tv_tableam_handler);
TableAmRoutine tvam_methods = {0};

/* DDL operations */
static void
tv_heapam_relation_set_new_filenode(Relation rel,
								 const RelFileNode *newrnode,
								 char persistence,
								 TransactionId *freezeXid,
								 MultiXactId *minmulti)
{
	SMgrRelation srel;

	if (persistence != RELPERSISTENCE_TEMP)
	{
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("Table Variable AM supports Temp Tables only.")));
	}

	/*
	 * Initialize to the minimum XID that could put tuples in the table. We
	 * know that no xacts older than RecentXmin are still running, so that
	 * will do.
	 */
	*freezeXid = RecentXmin;

	/*
	 * Similarly, initialize the minimum Multixact to the first value that
	 * could possibly be stored in tuples in the table.  Running transactions
	 * could reuse values from their local cache, so we are careful to
	 * consider all currently running multis.
	 *
	 * XXX this could be refined further, but is it worth the hassle?
	 */
	*minmulti = GetOldestMultiXactId();

	/*
	* Table Variables are not sensitive to ROLLBACKs so do not delete the
	* table files on abort. (TODO: This table am should have its own cleanup mechanism)
	*/
	srel = RelationCreateStorage(*newrnode, persistence, false);

	/*
	 * If required, set up an init fork for an unlogged table so that it can
	 * be correctly reinitialized on restart.  An immediate sync is required
	 * even if the page has been logged, because the write did not go through
	 * shared_buffers and therefore a concurrent checkpoint may have moved the
	 * redo pointer past our xlog record.  Recovery may as well remove it
	 * while replaying, for example, XLOG_DBASE_CREATE* or XLOG_TBLSPC_CREATE
	 * record. Therefore, logging is necessary even if wal_level=minimal.
	 */
	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		Assert(rel->rd_rel->relkind == RELKIND_RELATION ||
			   rel->rd_rel->relkind == RELKIND_MATVIEW ||
			   rel->rd_rel->relkind == RELKIND_TOASTVALUE);
		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(newrnode, INIT_FORKNUM);
		smgrimmedsync(srel, INIT_FORKNUM);
	}

	smgrclose(srel);
}

static bool
tv_heapam_tuple_satisfies_visibility(void *tuple, Snapshot snapshot, Buffer buffer)
{
	Assert(BufferIsValid(buffer));
	return TVHeapTupleSatisfiesVisibility((HeapTuple) tuple, snapshot, buffer);
}

static TM_Result
tv_heapam_tuple_satisfies_update(void *tuple, CommandId curcid, Buffer buffer)
{
	Assert(BufferIsValid(buffer));
	return TVHeapTupleSatisfiesUpdate((HeapTuple) tuple, curcid, buffer);
}

Datum tv_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&tvam_methods);
}

void _PG_init()
{
	static bool inited = false;
	Datum heapam_handler;
	TableAmRoutine *heap_routine = NULL;

	if (inited)
		return;

	/* Table Variable AM is based on Heap with some differences */
	heapam_handler = heap_tableam_handler((Datum) 0);
	heap_routine = (TableAmRoutine *) DatumGetPointer(heapam_handler);
	Assert(heap_routine);

	memcpy((void *) &tvam_methods, heap_routine, sizeof(TableAmRoutine));
	Assert(tvam_methods.type == T_TableAmRoutine);

	/* Register Table Variable AM specific functions here */
	tvam_methods.relation_set_new_filenode = tv_heapam_relation_set_new_filenode;
	tvam_methods.tuple_satisfies_visibility = tv_heapam_tuple_satisfies_visibility;
	tvam_methods.tuple_satisfies_update = tv_heapam_tuple_satisfies_update;

	inited = true;
}