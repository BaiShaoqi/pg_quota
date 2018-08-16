/* -------------------------------------------------------------------------
 *
 * fs_model.c
 *		In-memory data structures to track disk space usage of all relations.
 *
 * Copyright (c) 2013-2018, PostgreSQL Global Development Group
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/transam.h"
#include "catalog/pg_tablespace_d.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/ilist.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/relfilenode.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#include "pg_quota.h"

#define MAX_DB_ROLE_ENTRIES 1024

PG_FUNCTION_INFO_V1(get_quota_status);

typedef struct FileSizeEntry FileSizeEntry;
typedef struct RelSizeEntry RelSizeEntry;
typedef struct RelationSizeEntry RelationSizeEntry;
typedef struct RelationSizeEntryKey RelationSizeEntryKey;

/*
 * Shared memory structure.
 *
 * In shared memory, we keep a hash table of RelationSizeEntrys. It's keyed by
 * relid and database OID, and protected by shared->lock. It holds the
 * current total disk space usage, and quota, for each relation and database.
 */
struct RelationSizeEntryKey
{
	/* hash key consists of relation and database OID */
	Oid			relid;
	Oid			dbid;
};

struct RelationSizeEntry
{
	RelationSizeEntryKey key;

	off_t		totalsize;	/* current total space usage */
	int64		quota;		/* quota from config table */
};

static HTAB *relation_totals_map;

typedef struct
{
	LWLock	   *lock;		/* protects relation_totals_map */
} pg_quota_shared_state;

static pg_quota_shared_state *shared;


static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static Size pg_quota_memsize(void);
static void pg_quota_shmem_startup(void);


/*
 * Per-worker initialization. Create local hashes.
 */
void
init_fs_model(void)
{
	HASH_SEQ_STATUS iter;
	RelationSizeEntry *relentry;

	/*
	 * Remove any old entries for this database from the shared memory hash
	 * table, in case an old worker died and left them behind.
	 */
	LWLockAcquire(shared->lock, LW_EXCLUSIVE);

	hash_seq_init(&iter, relation_totals_map);

	while ((relentry = hash_seq_search(&iter)) != NULL)
	{
		/* only reset entries for current db */
		if (relentry->key.dbid == MyDatabaseId)
		{
			(void) hash_search(relation_totals_map,
							   (void *) relentry,
							   HASH_REMOVE, NULL);
		}
	}
	LWLockRelease(shared->lock);
}

void
init_fs_model_shmem(void)
{
	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in pgss_shmem_startup().
	 */
	RequestAddinShmemSpace(pg_quota_memsize());
	RequestNamedLWLockTranche("pg_quota", 1);

	/*
	 * Install startup hook to initialize our shared memory.
	 */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pg_quota_shmem_startup;
}

/* Estimate shared memory space needed. */
static Size
pg_quota_memsize(void)
{
	Size		size;

	size = MAXALIGN(sizeof(pg_quota_shared_state));
	size = add_size(size, hash_estimate_size(MAX_DB_ROLE_ENTRIES,
											 sizeof(RelationSizeEntry)));
	return size;
}

/*
 * Initialize shared memory.
 */
static void
pg_quota_shmem_startup(void)
{
	HASHCTL		hash_ctl;
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case this is a restart within the postmaster */
	shared = NULL;
	relation_totals_map = NULL;

	/*
	 * The RelationSizeEntry hash table is kept in shared memory, so that backends
	 * can do lookups in it.
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	shared = ShmemInitStruct("pg_quota",
							 sizeof(pg_quota_shared_state),
							 &found);
	if (!found)
	{
		shared->lock = &(GetNamedLWLockTranche("pg_quota"))->lock;
	}

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(RelationSizeEntryKey);
	hash_ctl.entrysize = sizeof(RelationSizeEntry);
	relation_totals_map = ShmemInitHash("relation OID to RelationSizeEntry map",
									MAX_DB_ROLE_ENTRIES,
									MAX_DB_ROLE_ENTRIES,
									&hash_ctl,
									HASH_ELEM | HASH_BLOBS);

	LWLockRelease(AddinShmemInitLock);
}


/*
 * This update the quota field and update relation size in the in-memory model.
 */
void
UpdateQuotaRefreshRelationSize(Oid relid, int64 newquota, int64 newtotalsize)
{
	RelationSizeEntry *relentry;
	RelationSizeEntryKey key;
	bool		found;

	LWLockAcquire(shared->lock, LW_EXCLUSIVE);

	key.relid = relid;
	key.dbid = MyDatabaseId;
	relentry = (RelationSizeEntry *) hash_search(relation_totals_map,
											 (void *) &key,
											 HASH_ENTER, &found);
	relentry->quota = newquota;
	relentry->totalsize = newtotalsize;

	LWLockRelease(shared->lock);
}


/* ---------------------------------------------------------------------------
 * Functions for use in backend processes.
 * ---------------------------------------------------------------------------
 */

/*
 * Returns 'true', if the quota for 'relation' has not been exceeded yet.
 */
bool
CheckQuota(Oid relid)
{
	RelationSizeEntry *relentry;
	RelationSizeEntryKey key;
	bool		result;

	if (!relation_totals_map)
		return true;

	LWLockAcquire(shared->lock, LW_SHARED);

	key.relid = relid;
	key.dbid = MyDatabaseId;
	relentry = (RelationSizeEntry *) hash_search(relation_totals_map,
											 (void *) &key,
											 HASH_FIND, NULL);
	if (relentry &&
		relentry->quota >= 0 &&
		relentry->totalsize > relentry->quota)
	{
		/* Relation has a quota, and it's been exceeded. */
		result = false;
	}
	else
	{
		result = true;
	}

	LWLockRelease(shared->lock);

	return result;
}

/*
 * Function to implement the quota.status view.
 */
Datum
get_quota_status(PG_FUNCTION_ARGS)
{
#define GET_QUOTA_STATUS_COLS	3
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	HASH_SEQ_STATUS iter;
	RelationSizeEntry *relentry;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	if (relation_totals_map)
	{
		LWLockAcquire(shared->lock, LW_SHARED);

		hash_seq_init(&iter, relation_totals_map);
		while ((relentry = hash_seq_search(&iter)) != NULL)
		{
			/* for each row */
			Datum		values[GET_QUOTA_STATUS_COLS];
			bool		nulls[GET_QUOTA_STATUS_COLS];

			/* Ignore entries for other databases. */
			if (relentry->key.dbid != MyDatabaseId)
				continue;

			values[0] = relentry->key.relid;
			nulls[0] = false;
			values[1] = relentry->totalsize;
			nulls[1] = false;
			if (relentry->quota != -1)
			{
				values[2] = relentry->quota;
				nulls[2] = false;
			}
			else
			{
				values[2] = (Datum) 0;
				nulls[2] = true;
			}

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}

		LWLockRelease(shared->lock);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}
