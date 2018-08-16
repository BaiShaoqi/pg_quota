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
#include "executor/executor.h"
#include "utils/syscache.h"

#include "pg_quota.h"

static bool quota_check_ExecCheckRTPerms(List *rangeTable, bool ereport_on_violation);

static ExecutorCheckPerms_hook_type prev_ExecutorCheckPerms_hook;
static bool ExecutorCheckPerms_hook_installed = false;

/*
 * Initialize enforcement, by installing the executor permission hook.
 */
void
init_quota_enforcement(void)
{
	if (!ExecutorCheckPerms_hook_installed)
	{
		prev_ExecutorCheckPerms_hook = ExecutorCheckPerms_hook;
		ExecutorCheckPerms_hook = quota_check_ExecCheckRTPerms;

		elog(DEBUG1, "disk quota permissions hook installed");
	}
}

/*
 * Permission check hook function. Throws an error if you try to INSERT
 * (or COPY) into a table, and the quota has been exceeded.
 */
static bool
quota_check_ExecCheckRTPerms(List *rangeTable, bool ereport_on_violation)
{
	ListCell   *l;

	foreach(l, rangeTable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(l);

		/* see ExecCheckRTEPerms() */
		if (rte->rtekind != RTE_RELATION)
			continue;

		/*
		 * Only check quota on inserts. UPDATEs may well increase
		 * space usage too, but we ignore that for now.
		 */
		if ((rte->requiredPerms & ACL_INSERT) == 0)
			continue;


		if (!CheckQuota(rte->relid))
		{
			/*
			 * The relation is out of quota. Report error.
			 *
			 * We
			 */
			if (ereport_on_violation)
				ereport(ERROR,
						(errcode(ERRCODE_DISK_FULL),
						 errmsg("table's disk space quota exceeded")));
			return false;
		}
	}

	return true;
}
