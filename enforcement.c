/* -------------------------------------------------------------------------
 *
 * enforcement.c
 *		Hooks to enforce the disk space quotas.
 *
 * This file contains functions for enforcing quotas. Currently, they are
 * only enforced for INSERTS and COPY, by using the ExecCheckRTPerms hook.
 *
 * Copyright (c) 2013-2018, PostgreSQL Global Development Group
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "storage/smgr.h"
#include "utils/syscache.h"

#include "pg_quota.h"

static void quota_check_SmgrExtend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer, bool skipFsync);

static SmgrExtend_hook_type prev_SmgrExtend_hook;
static bool SmgrExtend_hook_installed = false;

/*
 * Initialize enforcement, by installing the executor permission hook.
 */
void
init_quota_enforcement(void)
{
	if (!SmgrExtend_hook_installed)
	{
		prev_SmgrExtend_hook = SmgrExtend_hook;
		SmgrExtend_hook = quota_check_SmgrExtend;

		elog(DEBUG1, "disk quota SmgrExtend hook installed");
	}
}

static Oid
get_rel_owner(Oid relid)
{
	HeapTuple	tp;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
		Oid			result;

		result = reltup->relowner;
		ReleaseSysCache(tp);
		return result;
	}
	else
	{
		elog(DEBUG1, "could not find owner for relation %u", relid);
		return InvalidOid;
	}
}

/*
 * Permission check hook function. Throws an error if you try to INSERT
 * (or COPY) into a table, and the quota has been exceeded.
 */
static void
quota_check_SmgrExtend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer, bool skipFsync)
{
	Oid owner;
		
	owner = get_rel_owner(reln->smgr_rnode.node.relNode);
	if (owner == InvalidOid)
		return; /* no owner, huh? */

	if (!CheckQuota(owner))
	{
		/*
		 * The owner is out of quota. Report error.
		 *
		 * We
		 */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				errmsg("user's disk space quota exceeded")));
	}

	return;
}
