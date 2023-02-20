
#include "postgres.h"

#include "tv_heapam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/subtrans.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/combocid.h"
#include "utils/snapmgr.h"

static bool
TVHeapTupleSatisfiesAny(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
    return true;
}

/*
 * TVHeapTupleSatisfiesMVCC
 * 
 * Table Variables are not  sensitive to rollbacks
 * and are meant for use on current session only.
 */
static bool
TVHeapTupleSatisfiesMVCC(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
    HeapTupleHeader tuple = htup->t_data;

    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);

    if (!HeapTupleHeaderXminCommitted(tuple))
    {
        if (HeapTupleHeaderXminInvalid(tuple))
            return false;

        else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
        {
            if (HeapTupleHeaderGetCmin(tuple) >= snapshot->curcid)
                return false;	/* inserted after scan started */

            if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
                return true;

            if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
                return true;

            if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
            {
                TransactionId xmax;

                xmax = HeapTupleGetUpdateXid(tuple);

                /* not LOCKED_ONLY, so it has to have an xmax */
                Assert(TransactionIdIsValid(xmax));

                /* updating subtransaction must have aborted */
                if (!TransactionIdIsCurrentTransactionId(xmax))
                    return true;
                else if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
                    return true;	/* updated after scan started */
                else
                    return false;	/* updated before scan started */
            }

            if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
            {
                /* deleting subtransaction must have aborted */
                // SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                // 			InvalidTransactionId);
                //return true;
                return false;       // TableVariableAM is not sensitive to rollback
            }

            if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
                return true;	/* deleted after scan started */
            else
                return false;	/* deleted before scan started */
        }
        else if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
            return false;
        else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
        {
            // SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
            // 			HeapTupleHeaderGetRawXmin(tuple));
            ;
        }
        else
        {
            /* xmin is aborted or crashed - check xmax */
            //return true;
        }
    }
    else
    {
        /* xmin is committed, but maybe not according to our snapshot */
        if (!HeapTupleHeaderXminFrozen(tuple) &&
            XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
            return false;		/* treat as still in progress */
    }

    /* by here, the inserting transaction has committed */

    if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
        return true;

    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
        return true;

    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
    {
        TransactionId xmax;

        /* already checked above */
        Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

        xmax = HeapTupleGetUpdateXid(tuple);

        /* not LOCKED_ONLY, so it has to have an xmax */
        Assert(TransactionIdIsValid(xmax));

        if (TransactionIdIsCurrentTransactionId(xmax))
        {
            if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
                return true;	/* deleted after scan started */
            else
                return false;	/* deleted before scan started */
        }
        if (XidInMVCCSnapshot(xmax, snapshot))
            return true;
        if (TransactionIdDidCommit(xmax))
            return false;		/* updating transaction committed */
        /* it must have aborted or crashed */
        return true;
    }

    if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
    {
        if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
        {
            if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
                return true;	/* deleted after scan started */
            else
                return false;	/* deleted before scan started */
        }

        if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
            return true;

        if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)) )
        {
            /* xmax must have aborted or crashed */
            // SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
            // 			InvalidTransactionId);
            //return true;
            return false;
        }

        /* xmax transaction committed */
        // SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
        // 			HeapTupleHeaderGetRawXmax(tuple));
    }
    else
    {
        /* xmax is committed, but maybe not according to our snapshot */
        if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
            return true;		/* treat as still in progress */
    }

    /* xmax transaction committed */

    return false;
}


/*
 * HeapTupleSatisfiesVisibility
 *		True iff heap tuple satisfies a time qual.
 *
 * Notes:
 *	Assumes heap tuple is valid, and buffer at least share locked.
 *
 *	Hint bits in the HeapTuple's t_infomask may be updated as a side effect;
 *	if so, the indicated buffer is marked dirty.
 */
bool
TVHeapTupleSatisfiesVisibility(HeapTuple tup, Snapshot snapshot, Buffer buffer)
{
    switch (snapshot->snapshot_type)
    {
        case SNAPSHOT_MVCC:
            return TVHeapTupleSatisfiesMVCC(tup, snapshot, buffer);
            break;
        case SNAPSHOT_ANY:
            return TVHeapTupleSatisfiesAny(tup, snapshot, buffer);
            break;

        // TableVariableAMTODO: GO through each and make sure they are all rollback insensitive
        // case SNAPSHOT_SELF:
        //     return HeapTupleSatisfiesSelf(tup, snapshot, buffer);
        //     break;
        // case SNAPSHOT_TOAST:
        //     return HeapTupleSatisfiesToast(tup, snapshot, buffer);
        //     break;
        // case SNAPSHOT_DIRTY:
        //     return HeapTupleSatisfiesDirty(tup, snapshot, buffer);
        //     break;
        // case SNAPSHOT_HISTORIC_MVCC:
        //     return HeapTupleSatisfiesHistoricMVCC(tup, snapshot, buffer);
        //     break;
        // case SNAPSHOT_NON_VACUUMABLE:
        //     return HeapTupleSatisfiesNonVacuumable(tup, snapshot, buffer);
        //     break;
        case SNAPSHOT_SELF:
        case SNAPSHOT_TOAST:
        case SNAPSHOT_DIRTY:
        case SNAPSHOT_HISTORIC_MVCC:
        case SNAPSHOT_NON_VACUUMABLE:
            ereport(WARNING, (errmsg("[TableVariableAM] Unsupported snapshot type %d", snapshot->snapshot_type)));
            break;
    }

    return false;				/* keep compiler quiet */

}


/*
 * TVHeapTupleSatisfiesUpdate
 *
 * Counterpart of HeapTupleSatisfiesUpdate.
 * The only difference is this function is not rollback sensitive.
 */
TM_Result
TVHeapTupleSatisfiesUpdate(HeapTuple htup, CommandId curcid,
                         Buffer buffer)
{
    HeapTupleHeader tuple = htup->t_data;

    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);

    if (!HeapTupleHeaderXminCommitted(tuple))
    {
        if (HeapTupleHeaderXminInvalid(tuple))
            return TM_Invisible;

        else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
        {
            if (HeapTupleHeaderGetCmin(tuple) >= curcid)
                return TM_Invisible;    /* inserted after scan started */

            if (tuple->t_infomask & HEAP_XMAX_INVALID)  /* xid invalid */
                return TM_Ok;

            if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            {
                TransactionId xmax;

                xmax = HeapTupleHeaderGetRawXmax(tuple);

                /*
                 * Careful here: even though this tuple was created by our own
                 * transaction, it might be locked by other transactions, if
                 * the original version was key-share locked when we updated
                 * it.
                 */

                if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
                {
                    if (MultiXactIdIsRunning(xmax, true))
                        return TM_BeingModified;
                    else
                        return TM_Ok;
                }

                /*
                 * If the locker is gone, then there is nothing of interest
                 * left in this Xmax; otherwise, report the tuple as
                 * locked/updated.
                 */
                if (!TransactionIdIsInProgress(xmax))
                    return TM_Ok;
                return TM_BeingModified;
            }

            if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
            {
                TransactionId xmax;

                xmax = HeapTupleGetUpdateXid(tuple);

                /* not LOCKED_ONLY, so it has to have an xmax */
                Assert(TransactionIdIsValid(xmax));

                /* deleting subtransaction must have aborted */
                if (!TransactionIdIsCurrentTransactionId(xmax))
                {
                    ereport(PANIC, (errmsg("Table Variable AM should not get here")));

                    if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple),
                                             false))
                        return TM_BeingModified;
                    return TM_Ok;
                }
                else
                {
                    if (HeapTupleHeaderGetCmax(tuple) >= curcid)
                        return TM_SelfModified; /* updated after scan started */
                    else
                        return TM_Invisible;    /* updated before scan started */
                }
            }

            if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
            {
                /* deleting subtransaction must have aborted */
                // SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                //             InvalidTransactionId);
                //return TM_Ok;
                return TM_Invisible; // Table Variable AM is not sensitive to rollback
            }

            if (HeapTupleHeaderGetCmax(tuple) >= curcid)
                return TM_SelfModified; /* updated after scan started */
            else
                return TM_Invisible;    /* updated before scan started */
        }
        else if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)))
            return TM_Invisible;
        else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
        {
            // SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
            //             HeapTupleHeaderGetRawXmin(tuple));
            ;
        }
            
        else
        {
            /* it must have aborted or crashed */
            // SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
            //             InvalidTransactionId);
            // return TM_Invisible;
            return TM_Ok; // Table Variable AM is not sensitive to rollback
        }
    }

    /* by here, the inserting transaction has committed */

    if (tuple->t_infomask & HEAP_XMAX_INVALID)  /* xid invalid or aborted */
        return TM_Ok;

    if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
    {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return TM_Ok;
        if (!ItemPointerEquals(&htup->t_self, &tuple->t_ctid))
            return TM_Updated;  /* updated by other */
        else
            return TM_Deleted;  /* deleted by other */
    }

    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
    {
        TransactionId xmax;

        if (HEAP_LOCKED_UPGRADED(tuple->t_infomask))
            return TM_Ok;

        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
        {
            if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), true))
                return TM_BeingModified;

            //SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
            return TM_Ok;
        }

        xmax = HeapTupleGetUpdateXid(tuple);
        if (!TransactionIdIsValid(xmax))
        {
            if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
                return TM_BeingModified;
        }

        /* not LOCKED_ONLY, so it has to have an xmax */
        Assert(TransactionIdIsValid(xmax));

        if (TransactionIdIsCurrentTransactionId(xmax))
        {
            if (HeapTupleHeaderGetCmax(tuple) >= curcid)
                return TM_SelfModified; /* updated after scan started */
            else
                return TM_Invisible;    /* updated before scan started */
        }

        if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
            return TM_BeingModified;

        if (TransactionIdDidCommit(xmax))
        {
            if (!ItemPointerEquals(&htup->t_self, &tuple->t_ctid))
                return TM_Updated;
            else
                return TM_Deleted;
        }

         /*
         * By here, the update in the Xmax is either aborted or crashed, but
         * what about the other members?
         */

        if (!MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
        {
            /*
             * There's no member, even just a locker, alive anymore, so we can
             * mark the Xmax as invalid.
             */
            // SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
            //             InvalidTransactionId);
            return TM_Ok;
        }
        else
        {
            /* There are lockers running */
            return TM_BeingModified;
        }
    }

    if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
    {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return TM_BeingModified;
        if (HeapTupleHeaderGetCmax(tuple) >= curcid)
            return TM_SelfModified; /* updated after scan started */
        else
            return TM_Invisible;    /* updated before scan started */
    }

    if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
        return TM_BeingModified;

    if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
    {
        /* it must have aborted or crashed */
        //SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
        //            InvalidTransactionId);
        //return TM_Ok;
        // Table Variable AM is not sensitive to rollback
        if (!ItemPointerEquals(&htup->t_self, &tuple->t_ctid))
            return TM_Updated;      /* updated by other */
        else
            return TM_Deleted;      /* deleted by other */
    }

    /* xmax transaction committed */

    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
    {
        // SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
        //             InvalidTransactionId);
        return TM_Ok;
    }

    // SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
    //             HeapTupleHeaderGetRawXmax(tuple));
    if (!ItemPointerEquals(&htup->t_self, &tuple->t_ctid))
        return TM_Updated;      /* updated by other */
    else
        return TM_Deleted;      /* deleted by other */
}
