/*-------------------------------------------------------------------------
 *
 * common.c
 *	Catalog routines used by pg_dump; long ago these were shared
 *	by another dump tool, but not anymore.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/bin/pg_dump/common.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <ctype.h>

#include "catalog/pg_class_d.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_extension_d.h"
#include "catalog/pg_namespace_d.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_proc_d.h"
#include "catalog/pg_publication_d.h"
#include "catalog/pg_type_d.h"
#include "common/hashfn.h"
#include "fe_utils/string_utils.h"
#include "pg_backup_archiver.h"
#include "pg_backup_utils.h"
#include "pg_dump.h"

/*
 * Variables for mapping DumpId to DumpableObject
 */
static DumpableObject **dumpIdMap = NULL;
static int	allocedDumpIds = 0;
static DumpId lastDumpId = 0;	/* Note: 0 is InvalidDumpId */

/*
 * Infrastructure for mapping CatalogId to DumpableObject
 *
 * We use a hash table generated by simplehash.h.  That infrastructure
 * requires all the hash table entries to be the same size, and it also
 * expects that it can move them around when resizing the table.  So we
 * cannot make the DumpableObjects be elements of the hash table directly;
 * instead, the hash table elements contain pointers to DumpableObjects.
 *
 * It turns out to be convenient to also use this data structure to map
 * CatalogIds to owning extensions, if any.  Since extension membership
 * data is read before creating most DumpableObjects, either one of dobj
 * and ext could be NULL.
 */
typedef struct _catalogIdMapEntry
{
	CatalogId	catId;			/* the indexed CatalogId */
	uint32		status;			/* hash status */
	uint32		hashval;		/* hash code for the CatalogId */
	DumpableObject *dobj;		/* the associated DumpableObject, if any */
	ExtensionInfo *ext;			/* owning extension, if any */
} CatalogIdMapEntry;

#define SH_PREFIX		catalogid
#define SH_ELEMENT_TYPE	CatalogIdMapEntry
#define SH_KEY_TYPE		CatalogId
#define	SH_KEY			catId
#define SH_HASH_KEY(tb, key)	hash_bytes((const unsigned char *) &(key), sizeof(CatalogId))
#define SH_EQUAL(tb, a, b)		((a).oid == (b).oid && (a).tableoid == (b).tableoid)
#define SH_STORE_HASH
#define SH_GET_HASH(tb, a) (a)->hashval
#define	SH_SCOPE		static inline
#define SH_RAW_ALLOCATOR	pg_malloc0
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

#define CATALOGIDHASH_INITIAL_SIZE	10000

static catalogid_hash *catalogIdHash = NULL;

static void flagInhTables(Archive *fout, TableInfo *tblinfo, int numTables,
						  InhInfo *inhinfo, int numInherits);
static void flagInhIndexes(Archive *fout, TableInfo *tblinfo, int numTables);
static void flagInhAttrs(Archive *fout, DumpOptions *dopt, TableInfo *tblinfo,
						 int numTables);
static int	strInArray(const char *pattern, char **arr, int arr_size);
static IndxInfo *findIndexByOid(Oid oid);


/*
 * getSchemaData
 *	  Collect information about all potentially dumpable objects
 */
TableInfo *
getSchemaData(Archive *fout, int *numTablesPtr)
{
	TableInfo  *tblinfo;
	ExtensionInfo *extinfo;
	InhInfo    *inhinfo;
	int			numTables;
	int			numTypes;
	int			numFuncs;
	int			numOperators;
	int			numCollations;
	int			numNamespaces;
	int			numExtensions;
	int			numPublications;
	int			numAggregates;
	int			numInherits;
	int			numRules;
	int			numProcLangs;
	int			numCasts;
	int			numTransforms;
	int			numAccessMethods;
	int			numOpclasses;
	int			numOpfamilies;
	int			numConversions;
	int			numTSParsers;
	int			numTSTemplates;
	int			numTSDicts;
	int			numTSConfigs;
	int			numForeignDataWrappers;
	int			numForeignServers;
	int			numDefaultACLs;
	int			numEventTriggers;

	/*
	 * We must read extensions and extension membership info first, because
	 * extension membership needs to be consultable during decisions about
	 * whether other objects are to be dumped.
	 */
	pg_log_info("reading extensions");
	extinfo = getExtensions(fout, &numExtensions);

	pg_log_info("identifying extension members");
	getExtensionMembership(fout, extinfo, numExtensions);

	pg_log_info("reading schemas");
	(void) getNamespaces(fout, &numNamespaces);

	/*
	 * getTables should be done as soon as possible, so as to minimize the
	 * window between starting our transaction and acquiring per-table locks.
	 * However, we have to do getNamespaces first because the tables get
	 * linked to their containing namespaces during getTables.
	 */
	pg_log_info("reading user-defined tables");
	tblinfo = getTables(fout, &numTables);

	getOwnedSeqs(fout, tblinfo, numTables);

	pg_log_info("reading user-defined functions");
	(void) getFuncs(fout, &numFuncs);

	/* this must be after getTables and getFuncs */
	pg_log_info("reading user-defined types");
	(void) getTypes(fout, &numTypes);

	/* this must be after getFuncs, too */
	pg_log_info("reading procedural languages");
	getProcLangs(fout, &numProcLangs);

	pg_log_info("reading user-defined aggregate functions");
	getAggregates(fout, &numAggregates);

	pg_log_info("reading user-defined operators");
	(void) getOperators(fout, &numOperators);

	pg_log_info("reading user-defined access methods");
	getAccessMethods(fout, &numAccessMethods);

	pg_log_info("reading user-defined operator classes");
	getOpclasses(fout, &numOpclasses);

	pg_log_info("reading user-defined operator families");
	getOpfamilies(fout, &numOpfamilies);

	pg_log_info("reading user-defined text search parsers");
	getTSParsers(fout, &numTSParsers);

	pg_log_info("reading user-defined text search templates");
	getTSTemplates(fout, &numTSTemplates);

	pg_log_info("reading user-defined text search dictionaries");
	getTSDictionaries(fout, &numTSDicts);

	pg_log_info("reading user-defined text search configurations");
	getTSConfigurations(fout, &numTSConfigs);

	pg_log_info("reading user-defined foreign-data wrappers");
	getForeignDataWrappers(fout, &numForeignDataWrappers);

	pg_log_info("reading user-defined foreign servers");
	getForeignServers(fout, &numForeignServers);

	pg_log_info("reading default privileges");
	getDefaultACLs(fout, &numDefaultACLs);

	pg_log_info("reading user-defined collations");
	(void) getCollations(fout, &numCollations);

	pg_log_info("reading user-defined conversions");
	getConversions(fout, &numConversions);

	pg_log_info("reading type casts");
	getCasts(fout, &numCasts);

	pg_log_info("reading transforms");
	getTransforms(fout, &numTransforms);

	pg_log_info("reading table inheritance information");
	inhinfo = getInherits(fout, &numInherits);

	pg_log_info("reading event triggers");
	getEventTriggers(fout, &numEventTriggers);

	/* Identify extension configuration tables that should be dumped */
	pg_log_info("finding extension tables");
	processExtensionTables(fout, extinfo, numExtensions);

	/* Link tables to parents, mark parents of target tables interesting */
	pg_log_info("finding inheritance relationships");
	flagInhTables(fout, tblinfo, numTables, inhinfo, numInherits);

	pg_log_info("reading column info for interesting tables");
	getTableAttrs(fout, tblinfo, numTables);

	pg_log_info("flagging inherited columns in subtables");
	flagInhAttrs(fout, fout->dopt, tblinfo, numTables);

	pg_log_info("reading partitioning data");
	getPartitioningInfo(fout);

	pg_log_info("reading indexes");
	getIndexes(fout, tblinfo, numTables);

	pg_log_info("flagging indexes in partitioned tables");
	flagInhIndexes(fout, tblinfo, numTables);

	pg_log_info("reading extended statistics");
	getExtendedStatistics(fout);

	pg_log_info("reading constraints");
	getConstraints(fout, tblinfo, numTables);

	pg_log_info("reading triggers");
	getTriggers(fout, tblinfo, numTables);

	pg_log_info("reading rewrite rules");
	getRules(fout, &numRules);

	pg_log_info("reading policies");
	getPolicies(fout, tblinfo, numTables);

	pg_log_info("reading publications");
	(void) getPublications(fout, &numPublications);

	pg_log_info("reading publication membership of tables");
	getPublicationTables(fout, tblinfo, numTables);

	pg_log_info("reading publication membership of schemas");
	getPublicationNamespaces(fout);

	pg_log_info("reading subscriptions");
	getSubscriptions(fout);

	free(inhinfo);				/* not needed any longer */

	*numTablesPtr = numTables;
	return tblinfo;
}

/* flagInhTables -
 *	 Fill in parent link fields of tables for which we need that information,
 *	 mark parents of target tables as interesting, and create
 *	 TableAttachInfo objects for partitioned tables with appropriate
 *	 dependency links.
 *
 * Note that only direct ancestors of targets are marked interesting.
 * This is sufficient; we don't much care whether they inherited their
 * attributes or not.
 *
 * modifies tblinfo
 */
static void
flagInhTables(Archive *fout, TableInfo *tblinfo, int numTables,
			  InhInfo *inhinfo, int numInherits)
{
	TableInfo  *child = NULL;
	TableInfo  *parent = NULL;
	int			i,
				j;

	/*
	 * Set up links from child tables to their parents.
	 *
	 * We used to attempt to skip this work for tables that are not to be
	 * dumped; but the optimizable cases are rare in practice, and setting up
	 * these links in bulk is cheaper than the old way.  (Note in particular
	 * that it's very rare for a child to have more than one parent.)
	 */
	for (i = 0; i < numInherits; i++)
	{
		/*
		 * Skip a hashtable lookup if it's same table as last time.  This is
		 * unlikely for the child, but less so for the parent.  (Maybe we
		 * should ask the backend for a sorted array to make it more likely?
		 * Not clear the sorting effort would be repaid, though.)
		 */
		if (child == NULL ||
			child->dobj.catId.oid != inhinfo[i].inhrelid)
		{
			child = findTableByOid(inhinfo[i].inhrelid);

			/*
			 * If we find no TableInfo, assume the pg_inherits entry is for a
			 * partitioned index, which we don't need to track.
			 */
			if (child == NULL)
				continue;
		}
		if (parent == NULL ||
			parent->dobj.catId.oid != inhinfo[i].inhparent)
		{
			parent = findTableByOid(inhinfo[i].inhparent);
			if (parent == NULL)
				pg_fatal("failed sanity check, parent OID %u of table \"%s\" (OID %u) not found",
						 inhinfo[i].inhparent,
						 child->dobj.name,
						 child->dobj.catId.oid);
		}
		/* Add this parent to the child's list of parents. */
		if (child->numParents > 0)
			child->parents = pg_realloc_array(child->parents,
											  TableInfo *,
											  child->numParents + 1);
		else
			child->parents = pg_malloc_array(TableInfo *, 1);
		child->parents[child->numParents++] = parent;
	}

	/*
	 * Now consider all child tables and mark parents interesting as needed.
	 */
	for (i = 0; i < numTables; i++)
	{
		/*
		 * If needed, mark the parents as interesting for getTableAttrs and
		 * getIndexes.  We only need this for direct parents of dumpable
		 * tables.
		 */
		if (tblinfo[i].dobj.dump)
		{
			int			numParents = tblinfo[i].numParents;
			TableInfo **parents = tblinfo[i].parents;

			for (j = 0; j < numParents; j++)
				parents[j]->interesting = true;
		}

		/* Create TableAttachInfo object if needed */
		if ((tblinfo[i].dobj.dump & DUMP_COMPONENT_DEFINITION) &&
			tblinfo[i].ispartition)
		{
			TableAttachInfo *attachinfo;

			/* With partitions there can only be one parent */
			if (tblinfo[i].numParents != 1)
				pg_fatal("invalid number of parents %d for table \"%s\"",
						 tblinfo[i].numParents,
						 tblinfo[i].dobj.name);

			attachinfo = (TableAttachInfo *) palloc(sizeof(TableAttachInfo));
			attachinfo->dobj.objType = DO_TABLE_ATTACH;
			attachinfo->dobj.catId.tableoid = 0;
			attachinfo->dobj.catId.oid = 0;
			AssignDumpId(&attachinfo->dobj);
			attachinfo->dobj.name = pg_strdup(tblinfo[i].dobj.name);
			attachinfo->dobj.namespace = tblinfo[i].dobj.namespace;
			attachinfo->parentTbl = tblinfo[i].parents[0];
			attachinfo->partitionTbl = &tblinfo[i];

			/*
			 * We must state the DO_TABLE_ATTACH object's dependencies
			 * explicitly, since it will not match anything in pg_depend.
			 *
			 * Give it dependencies on both the partition table and the parent
			 * table, so that it will not be executed till both of those
			 * exist.  (There's no need to care what order those are created
			 * in.)
			 */
			addObjectDependency(&attachinfo->dobj, tblinfo[i].dobj.dumpId);
			addObjectDependency(&attachinfo->dobj, tblinfo[i].parents[0]->dobj.dumpId);
		}
	}
}

/*
 * flagInhIndexes -
 *	 Create IndexAttachInfo objects for partitioned indexes, and add
 *	 appropriate dependency links.
 */
static void
flagInhIndexes(Archive *fout, TableInfo tblinfo[], int numTables)
{
	int			i,
				j;

	for (i = 0; i < numTables; i++)
	{
		if (!tblinfo[i].ispartition || tblinfo[i].numParents == 0)
			continue;

		Assert(tblinfo[i].numParents == 1);

		for (j = 0; j < tblinfo[i].numIndexes; j++)
		{
			IndxInfo   *index = &(tblinfo[i].indexes[j]);
			IndxInfo   *parentidx;
			IndexAttachInfo *attachinfo;

			if (index->parentidx == 0)
				continue;

			parentidx = findIndexByOid(index->parentidx);
			if (parentidx == NULL)
				continue;

			attachinfo = pg_malloc_object(IndexAttachInfo);

			attachinfo->dobj.objType = DO_INDEX_ATTACH;
			attachinfo->dobj.catId.tableoid = 0;
			attachinfo->dobj.catId.oid = 0;
			AssignDumpId(&attachinfo->dobj);
			attachinfo->dobj.name = pg_strdup(index->dobj.name);
			attachinfo->dobj.namespace = index->indextable->dobj.namespace;
			attachinfo->parentIdx = parentidx;
			attachinfo->partitionIdx = index;

			/*
			 * We must state the DO_INDEX_ATTACH object's dependencies
			 * explicitly, since it will not match anything in pg_depend.
			 *
			 * Give it dependencies on both the partition index and the parent
			 * index, so that it will not be executed till both of those
			 * exist.  (There's no need to care what order those are created
			 * in.)
			 *
			 * In addition, give it dependencies on the indexes' underlying
			 * tables.  This does nothing of great value so far as serial
			 * restore ordering goes, but it ensures that a parallel restore
			 * will not try to run the ATTACH concurrently with other
			 * operations on those tables.
			 */
			addObjectDependency(&attachinfo->dobj, index->dobj.dumpId);
			addObjectDependency(&attachinfo->dobj, parentidx->dobj.dumpId);
			addObjectDependency(&attachinfo->dobj,
								index->indextable->dobj.dumpId);
			addObjectDependency(&attachinfo->dobj,
								parentidx->indextable->dobj.dumpId);

			/* keep track of the list of partitions in the parent index */
			simple_ptr_list_append(&parentidx->partattaches, &attachinfo->dobj);
		}
	}
}

/* flagInhAttrs -
 *	 for each dumpable table in tblinfo, flag its inherited attributes
 *
 * What we need to do here is:
 *
 * - Detect child columns that inherit NOT NULL bits from their parents, so
 *   that we needn't specify that again for the child. (Versions >= 16 no
 *   longer need this.)
 *
 * - Detect child columns that have DEFAULT NULL when their parents had some
 *   non-null default.  In this case, we make up a dummy AttrDefInfo object so
 *   that we'll correctly emit the necessary DEFAULT NULL clause; otherwise
 *   the backend will apply an inherited default to the column.
 *
 * - Detect child columns that have a generation expression and all their
 *   parents also have the same generation expression, and if so suppress the
 *   child's expression.  The child will inherit the generation expression
 *   automatically, so there's no need to dump it.  This improves the dump's
 *   compatibility with pre-v16 servers, which didn't allow the child's
 *   expression to be given explicitly.  Exceptions: If it's a partition or
 *   we are in binary upgrade mode, we dump such expressions anyway because
 *   in those cases inherited tables are recreated standalone first and then
 *   reattached to the parent.  (See also the logic in dumpTableSchema().)
 *
 * modifies tblinfo
 */
static void
flagInhAttrs(Archive *fout, DumpOptions *dopt, TableInfo *tblinfo, int numTables)
{
	int			i,
				j,
				k;

	/*
	 * We scan the tables in OID order, since that's how tblinfo[] is sorted.
	 * Hence we will typically visit parents before their children --- but
	 * that is *not* guaranteed.  Thus this loop must be careful that it does
	 * not alter table properties in a way that could change decisions made at
	 * child tables during other iterations.
	 */
	for (i = 0; i < numTables; i++)
	{
		TableInfo  *tbinfo = &(tblinfo[i]);
		int			numParents;
		TableInfo **parents;

		/* Some kinds never have parents */
		if (tbinfo->relkind == RELKIND_SEQUENCE ||
			tbinfo->relkind == RELKIND_VIEW ||
			tbinfo->relkind == RELKIND_MATVIEW)
			continue;

		/* Don't bother computing anything for non-target tables, either */
		if (!tbinfo->dobj.dump)
			continue;

		numParents = tbinfo->numParents;
		parents = tbinfo->parents;

		if (numParents == 0)
			continue;			/* nothing to see here, move along */

		/* For each column, search for matching column names in parent(s) */
		for (j = 0; j < tbinfo->numatts; j++)
		{
			bool		foundNotNull;	/* Attr was NOT NULL in a parent */
			bool		foundDefault;	/* Found a default in a parent */
			bool		foundSameGenerated; /* Found matching GENERATED */
			bool		foundDiffGenerated; /* Found non-matching GENERATED */

			/* no point in examining dropped columns */
			if (tbinfo->attisdropped[j])
				continue;

			foundNotNull = false;
			foundDefault = false;
			foundSameGenerated = false;
			foundDiffGenerated = false;
			for (k = 0; k < numParents; k++)
			{
				TableInfo  *parent = parents[k];
				int			inhAttrInd;

				inhAttrInd = strInArray(tbinfo->attnames[j],
										parent->attnames,
										parent->numatts);
				if (inhAttrInd >= 0)
				{
					AttrDefInfo *parentDef = parent->attrdefs[inhAttrInd];

					foundNotNull |= (parent->notnull_constrs[inhAttrInd] != NULL &&
									 !parent->notnull_noinh[inhAttrInd]);
					foundDefault |= (parentDef != NULL &&
									 strcmp(parentDef->adef_expr, "NULL") != 0 &&
									 !parent->attgenerated[inhAttrInd]);
					if (parent->attgenerated[inhAttrInd])
					{
						/* these pointer nullness checks are just paranoia */
						if (parentDef != NULL &&
							tbinfo->attrdefs[j] != NULL &&
							strcmp(parentDef->adef_expr,
								   tbinfo->attrdefs[j]->adef_expr) == 0)
							foundSameGenerated = true;
						else
							foundDiffGenerated = true;
					}
				}
			}

			/* In versions < 17, remember if we found inherited NOT NULL */
			if (fout->remoteVersion < 170000)
				tbinfo->notnull_inh[j] = foundNotNull;

			/*
			 * Manufacture a DEFAULT NULL clause if necessary.  This breaks
			 * the advice given above to avoid changing state that might get
			 * inspected in other loop iterations.  We prevent trouble by
			 * having the foundDefault test above check whether adef_expr is
			 * "NULL", so that it will reach the same conclusion before or
			 * after this is done.
			 */
			if (foundDefault && tbinfo->attrdefs[j] == NULL)
			{
				AttrDefInfo *attrDef;

				attrDef = pg_malloc_object(AttrDefInfo);
				attrDef->dobj.objType = DO_ATTRDEF;
				attrDef->dobj.catId.tableoid = 0;
				attrDef->dobj.catId.oid = 0;
				AssignDumpId(&attrDef->dobj);
				attrDef->dobj.name = pg_strdup(tbinfo->dobj.name);
				attrDef->dobj.namespace = tbinfo->dobj.namespace;
				attrDef->dobj.dump = tbinfo->dobj.dump;

				attrDef->adtable = tbinfo;
				attrDef->adnum = j + 1;
				attrDef->adef_expr = pg_strdup("NULL");

				/* Will column be dumped explicitly? */
				if (shouldPrintColumn(dopt, tbinfo, j))
				{
					attrDef->separate = false;
					/* No dependency needed: NULL cannot have dependencies */
				}
				else
				{
					/* column will be suppressed, print default separately */
					attrDef->separate = true;
					/* ensure it comes out after the table */
					addObjectDependency(&attrDef->dobj,
										tbinfo->dobj.dumpId);
				}

				tbinfo->attrdefs[j] = attrDef;
			}

			/* No need to dump generation expression if it's inheritable */
			if (foundSameGenerated && !foundDiffGenerated &&
				!tbinfo->ispartition && !dopt->binary_upgrade)
				tbinfo->attrdefs[j]->dobj.dump = DUMP_COMPONENT_NONE;
		}
	}
}

/*
 * AssignDumpId
 *		Given a newly-created dumpable object, assign a dump ID,
 *		and enter the object into the lookup tables.
 *
 * The caller is expected to have filled in objType and catId,
 * but not any of the other standard fields of a DumpableObject.
 */
void
AssignDumpId(DumpableObject *dobj)
{
	dobj->dumpId = ++lastDumpId;
	dobj->name = NULL;			/* must be set later */
	dobj->namespace = NULL;		/* may be set later */
	dobj->dump = DUMP_COMPONENT_ALL;	/* default assumption */
	dobj->dump_contains = DUMP_COMPONENT_ALL;	/* default assumption */
	/* All objects have definitions; we may set more components bits later */
	dobj->components = DUMP_COMPONENT_DEFINITION;
	dobj->ext_member = false;	/* default assumption */
	dobj->depends_on_ext = false;	/* default assumption */
	dobj->dependencies = NULL;
	dobj->nDeps = 0;
	dobj->allocDeps = 0;

	/* Add object to dumpIdMap[], enlarging that array if need be */
	while (dobj->dumpId >= allocedDumpIds)
	{
		int			newAlloc;

		if (allocedDumpIds <= 0)
		{
			newAlloc = 256;
			dumpIdMap = pg_malloc_array(DumpableObject *, newAlloc);
		}
		else
		{
			newAlloc = allocedDumpIds * 2;
			dumpIdMap = pg_realloc_array(dumpIdMap, DumpableObject *, newAlloc);
		}
		memset(dumpIdMap + allocedDumpIds, 0,
			   (newAlloc - allocedDumpIds) * sizeof(DumpableObject *));
		allocedDumpIds = newAlloc;
	}
	dumpIdMap[dobj->dumpId] = dobj;

	/* If it has a valid CatalogId, enter it into the hash table */
	if (OidIsValid(dobj->catId.tableoid))
	{
		CatalogIdMapEntry *entry;
		bool		found;

		/* Initialize CatalogId hash table if not done yet */
		if (catalogIdHash == NULL)
			catalogIdHash = catalogid_create(CATALOGIDHASH_INITIAL_SIZE, NULL);

		entry = catalogid_insert(catalogIdHash, dobj->catId, &found);
		if (!found)
		{
			entry->dobj = NULL;
			entry->ext = NULL;
		}
		Assert(entry->dobj == NULL);
		entry->dobj = dobj;
	}
}

/*
 * Assign a DumpId that's not tied to a DumpableObject.
 *
 * This is used when creating a "fixed" ArchiveEntry that doesn't need to
 * participate in the sorting logic.
 */
DumpId
createDumpId(void)
{
	return ++lastDumpId;
}

/*
 * Return the largest DumpId so far assigned
 */
DumpId
getMaxDumpId(void)
{
	return lastDumpId;
}

/*
 * Find a DumpableObject by dump ID
 *
 * Returns NULL for invalid ID
 */
DumpableObject *
findObjectByDumpId(DumpId dumpId)
{
	if (dumpId <= 0 || dumpId >= allocedDumpIds)
		return NULL;			/* out of range? */
	return dumpIdMap[dumpId];
}

/*
 * Find a DumpableObject by catalog ID
 *
 * Returns NULL for unknown ID
 */
DumpableObject *
findObjectByCatalogId(CatalogId catalogId)
{
	CatalogIdMapEntry *entry;

	if (catalogIdHash == NULL)
		return NULL;			/* no objects exist yet */

	entry = catalogid_lookup(catalogIdHash, catalogId);
	if (entry == NULL)
		return NULL;
	return entry->dobj;
}

/*
 * Build an array of pointers to all known dumpable objects
 *
 * This simply creates a modifiable copy of the internal map.
 */
void
getDumpableObjects(DumpableObject ***objs, int *numObjs)
{
	int			i,
				j;

	*objs = pg_malloc_array(DumpableObject *, allocedDumpIds);
	j = 0;
	for (i = 1; i < allocedDumpIds; i++)
	{
		if (dumpIdMap[i])
			(*objs)[j++] = dumpIdMap[i];
	}
	*numObjs = j;
}

/*
 * Add a dependency link to a DumpableObject
 *
 * Note: duplicate dependencies are currently not eliminated
 */
void
addObjectDependency(DumpableObject *dobj, DumpId refId)
{
	if (dobj->nDeps >= dobj->allocDeps)
	{
		if (dobj->allocDeps <= 0)
		{
			dobj->allocDeps = 16;
			dobj->dependencies = pg_malloc_array(DumpId, dobj->allocDeps);
		}
		else
		{
			dobj->allocDeps *= 2;
			dobj->dependencies = pg_realloc_array(dobj->dependencies,
												  DumpId, dobj->allocDeps);
		}
	}
	dobj->dependencies[dobj->nDeps++] = refId;
}

/*
 * Remove a dependency link from a DumpableObject
 *
 * If there are multiple links, all are removed
 */
void
removeObjectDependency(DumpableObject *dobj, DumpId refId)
{
	int			i;
	int			j = 0;

	for (i = 0; i < dobj->nDeps; i++)
	{
		if (dobj->dependencies[i] != refId)
			dobj->dependencies[j++] = dobj->dependencies[i];
	}
	dobj->nDeps = j;
}


/*
 * findTableByOid
 *	  finds the DumpableObject for the table with the given oid
 *	  returns NULL if not found
 */
TableInfo *
findTableByOid(Oid oid)
{
	CatalogId	catId;
	DumpableObject *dobj;

	catId.tableoid = RelationRelationId;
	catId.oid = oid;
	dobj = findObjectByCatalogId(catId);
	Assert(dobj == NULL || dobj->objType == DO_TABLE);
	return (TableInfo *) dobj;
}

/*
 * findIndexByOid
 *	  finds the DumpableObject for the index with the given oid
 *	  returns NULL if not found
 */
static IndxInfo *
findIndexByOid(Oid oid)
{
	CatalogId	catId;
	DumpableObject *dobj;

	catId.tableoid = RelationRelationId;
	catId.oid = oid;
	dobj = findObjectByCatalogId(catId);
	Assert(dobj == NULL || dobj->objType == DO_INDEX);
	return (IndxInfo *) dobj;
}

/*
 * findTypeByOid
 *	  finds the DumpableObject for the type with the given oid
 *	  returns NULL if not found
 */
TypeInfo *
findTypeByOid(Oid oid)
{
	CatalogId	catId;
	DumpableObject *dobj;

	catId.tableoid = TypeRelationId;
	catId.oid = oid;
	dobj = findObjectByCatalogId(catId);
	Assert(dobj == NULL ||
		   dobj->objType == DO_TYPE || dobj->objType == DO_DUMMY_TYPE);
	return (TypeInfo *) dobj;
}

/*
 * findFuncByOid
 *	  finds the DumpableObject for the function with the given oid
 *	  returns NULL if not found
 */
FuncInfo *
findFuncByOid(Oid oid)
{
	CatalogId	catId;
	DumpableObject *dobj;

	catId.tableoid = ProcedureRelationId;
	catId.oid = oid;
	dobj = findObjectByCatalogId(catId);
	Assert(dobj == NULL || dobj->objType == DO_FUNC);
	return (FuncInfo *) dobj;
}

/*
 * findOprByOid
 *	  finds the DumpableObject for the operator with the given oid
 *	  returns NULL if not found
 */
OprInfo *
findOprByOid(Oid oid)
{
	CatalogId	catId;
	DumpableObject *dobj;

	catId.tableoid = OperatorRelationId;
	catId.oid = oid;
	dobj = findObjectByCatalogId(catId);
	Assert(dobj == NULL || dobj->objType == DO_OPERATOR);
	return (OprInfo *) dobj;
}

/*
 * findCollationByOid
 *	  finds the DumpableObject for the collation with the given oid
 *	  returns NULL if not found
 */
CollInfo *
findCollationByOid(Oid oid)
{
	CatalogId	catId;
	DumpableObject *dobj;

	catId.tableoid = CollationRelationId;
	catId.oid = oid;
	dobj = findObjectByCatalogId(catId);
	Assert(dobj == NULL || dobj->objType == DO_COLLATION);
	return (CollInfo *) dobj;
}

/*
 * findNamespaceByOid
 *	  finds the DumpableObject for the namespace with the given oid
 *	  returns NULL if not found
 */
NamespaceInfo *
findNamespaceByOid(Oid oid)
{
	CatalogId	catId;
	DumpableObject *dobj;

	catId.tableoid = NamespaceRelationId;
	catId.oid = oid;
	dobj = findObjectByCatalogId(catId);
	Assert(dobj == NULL || dobj->objType == DO_NAMESPACE);
	return (NamespaceInfo *) dobj;
}

/*
 * findExtensionByOid
 *	  finds the DumpableObject for the extension with the given oid
 *	  returns NULL if not found
 */
ExtensionInfo *
findExtensionByOid(Oid oid)
{
	CatalogId	catId;
	DumpableObject *dobj;

	catId.tableoid = ExtensionRelationId;
	catId.oid = oid;
	dobj = findObjectByCatalogId(catId);
	Assert(dobj == NULL || dobj->objType == DO_EXTENSION);
	return (ExtensionInfo *) dobj;
}

/*
 * findPublicationByOid
 *	  finds the DumpableObject for the publication with the given oid
 *	  returns NULL if not found
 */
PublicationInfo *
findPublicationByOid(Oid oid)
{
	CatalogId	catId;
	DumpableObject *dobj;

	catId.tableoid = PublicationRelationId;
	catId.oid = oid;
	dobj = findObjectByCatalogId(catId);
	Assert(dobj == NULL || dobj->objType == DO_PUBLICATION);
	return (PublicationInfo *) dobj;
}


/*
 * recordExtensionMembership
 *	  Record that the object identified by the given catalog ID
 *	  belongs to the given extension
 */
void
recordExtensionMembership(CatalogId catId, ExtensionInfo *ext)
{
	CatalogIdMapEntry *entry;
	bool		found;

	/* CatalogId hash table must exist, if we have an ExtensionInfo */
	Assert(catalogIdHash != NULL);

	/* Add reference to CatalogId hash */
	entry = catalogid_insert(catalogIdHash, catId, &found);
	if (!found)
	{
		entry->dobj = NULL;
		entry->ext = NULL;
	}
	Assert(entry->ext == NULL);
	entry->ext = ext;
}

/*
 * findOwningExtension
 *	  return owning extension for specified catalog ID, or NULL if none
 */
ExtensionInfo *
findOwningExtension(CatalogId catalogId)
{
	CatalogIdMapEntry *entry;

	if (catalogIdHash == NULL)
		return NULL;			/* no objects exist yet */

	entry = catalogid_lookup(catalogIdHash, catalogId);
	if (entry == NULL)
		return NULL;
	return entry->ext;
}


/*
 * parseOidArray
 *	  parse a string of numbers delimited by spaces into a character array
 *
 * Note: actually this is used for both Oids and potentially-signed
 * attribute numbers.  This should cause no trouble, but we could split
 * the function into two functions with different argument types if it does.
 */

void
parseOidArray(const char *str, Oid *array, int arraysize)
{
	int			j,
				argNum;
	char		temp[100];
	char		s;

	argNum = 0;
	j = 0;
	for (;;)
	{
		s = *str++;
		if (s == ' ' || s == '\0')
		{
			if (j > 0)
			{
				if (argNum >= arraysize)
					pg_fatal("could not parse numeric array \"%s\": too many numbers", str);
				temp[j] = '\0';
				array[argNum++] = atooid(temp);
				j = 0;
			}
			if (s == '\0')
				break;
		}
		else
		{
			if (!(isdigit((unsigned char) s) || s == '-') ||
				j >= sizeof(temp) - 1)
				pg_fatal("could not parse numeric array \"%s\": invalid character in number", str);
			temp[j++] = s;
		}
	}

	while (argNum < arraysize)
		array[argNum++] = InvalidOid;
}


/*
 * strInArray:
 *	  takes in a string and a string array and the number of elements in the
 * string array.
 *	  returns the index if the string is somewhere in the array, -1 otherwise
 */

static int
strInArray(const char *pattern, char **arr, int arr_size)
{
	int			i;

	for (i = 0; i < arr_size; i++)
	{
		if (strcmp(pattern, arr[i]) == 0)
			return i;
	}
	return -1;
}
