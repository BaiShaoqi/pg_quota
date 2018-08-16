/*
 *
 */

#ifndef PG_QUOTA_H
#define PG_QUOTA_H

#include "storage/relfilenode.h"


/* prototypes for fs_model.c */
extern void init_fs_model(void);
extern void init_fs_model_shmem(void);


extern bool CheckQuota(Oid relationid);
extern void UpdateQuotaRefreshRelationSize(Oid relationid, int64 newquota, int64 newtotalsize);

/* prototypes for enforcement.c */
extern void init_quota_enforcement(void);

#endif							/* PG_QUOTA_H */
