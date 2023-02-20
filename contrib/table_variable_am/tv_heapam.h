#ifndef TV_HEAPAM_H
#define TV_HEAPAM_H

#include "access/relation.h"
#include "access/tableam.h"
#include "storage/bufpage.h"
#include "utils/snapshot.h"

extern bool TVHeapTupleSatisfiesVisibility(HeapTuple tuple, Snapshot snapshot, Buffer buffer);
extern TM_Result TVHeapTupleSatisfiesUpdate(HeapTuple tuple, CommandId curcid, Buffer buffer);

#endif