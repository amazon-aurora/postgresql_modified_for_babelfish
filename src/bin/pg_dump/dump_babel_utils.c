/*-------------------------------------------------------------------------
 *
 * Utility routines for babelfish objects
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/pg_dump/dump_babel_utils.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "catalog/pg_class_d.h"
#include "catalog/pg_proc_d.h"
#include "catalog/pg_type_d.h"
#include "dump_babel_utils.h"
#include "pg_backup_db.h"
#include "pg_dump.h"
#include "pqexpbuffer.h"

static char *
get_language_name(Archive *fout, Oid langid)
{
	PQExpBuffer query;
	PGresult   *res;
	char	   *lanname;

	query = createPQExpBuffer();
	appendPQExpBuffer(query, "SELECT lanname FROM pg_language WHERE oid = %u", langid);
	res = ExecuteSqlQueryForSingleRow(fout, query->data);
	lanname = pg_strdup(PQgetvalue(res, 0, 0));
	destroyPQExpBuffer(query);
	PQclear(res);

	return lanname;
}

/*
 * bbf_selectDumpableCast: Mark a cast as to be dumped or not
 */
void
bbf_selectDumpableCast(CastInfo *cast)
{
	TypeInfo      *sTypeInfo;
	TypeInfo      *tTypeInfo;
	ExtensionInfo *ext = findOwningExtension(cast->dobj.catId);

	/* Skip if cast is not a member of babelfish extension */
	if (ext == NULL || strcmp(ext->dobj.name, "babelfishpg_common") != 0)
		return;

	sTypeInfo = findTypeByOid(cast->castsource);
	tTypeInfo = findTypeByOid(cast->casttarget);

	/*
	 * Do not dump following unused CASTS:
	 * pg_catalog.bool -> sys.bpchar
	 * pg_catalog.bool -> sys.varchar
	 */
	if (sTypeInfo && tTypeInfo &&
			sTypeInfo->dobj.namespace &&
			tTypeInfo->dobj.namespace &&
			strcmp(sTypeInfo->dobj.namespace->dobj.name, "pg_catalog") == 0 &&
			strcmp(tTypeInfo->dobj.namespace->dobj.name, "sys") == 0 &&
			strcmp(sTypeInfo->dobj.name, "bool") == 0 &&
			(strcmp(tTypeInfo->dobj.name, "bpchar") == 0 ||
			 strcmp(tTypeInfo->dobj.name, "varchar") == 0))
		cast->dobj.dump = DUMP_COMPONENT_NONE;
}

/*
 * bbf_fixTableTypeDependency:
 * Fixes following two types of dependency issues between T-SQL
 * table-type and T-SQL MS-TVF/procedure:
 * 1. T-SQL table-type has an INTERNAL dependency upon MS-TVF which
 *    is right thing for drop but creates dependency loop during
 *    pg_dump. Fix this by removing table-type's dependency on MS-TVF.
 * 2. By default function gets dumped before the template table of T-SQL
 *    table type(one of the datatype of function's arguments) which is
 *    because there is no dependency between function and underlying
 *    template table. Which is fine in normal case but becomes problematic
 *    during restore. Fix this by adding function's dependency on
 *    template table.
 */
void
bbf_fixTableTypeDependency(Archive *fout, DumpableObject *func, DumpableObject *tabletype, char deptype)
{
	FuncInfo  *funcInfo = (FuncInfo *) func;
	TypeInfo  *typeInfo = (TypeInfo *) tabletype;
	TableInfo *tytable;

	/* skip auto-generated array types and non-pltsql functions */
	if (typeInfo->isArray ||
		!OidIsValid(typeInfo->typrelid) ||
		strcmp(get_language_name(fout, funcInfo->lang), "pltsql") != 0)
		return;

	tytable = findTableByOid(typeInfo->typrelid);

	if (tytable == NULL)
		return;

	/* First case, so remove INTERNAL dependency between T-SQL table-type and MS-TVF */
	if (deptype == 'i')
		removeObjectDependency(tabletype, func->dumpId);
	/* Second case */
	else
		addObjectDependency(func, tytable->dobj.dumpId);
}

/*
 * bbf_isTsqlTableType:
 * Returns true if given table is a template table for
 * underlying T-SQL table-type, false otherwise.
 */
bool
bbf_isTsqlTableType(Archive *fout, const TableInfo *tbinfo)
{
	Oid			pg_type_oid;
	PQExpBuffer query;
	PGresult	*res;
	int			ntups;

	if(tbinfo->relkind != RELKIND_RELATION)
		return false;

	query = createPQExpBuffer();

	/* get oid of table's row type */
	appendPQExpBuffer(query,
					  "SELECT reltype "
					  "FROM pg_catalog.pg_class "
					  "WHERE relkind = '%c' "
					  "AND oid = '%u'::pg_catalog.oid;",
					  RELKIND_RELATION, tbinfo->dobj.catId.oid);

	res = ExecuteSqlQueryForSingleRow(fout, query->data);
	pg_type_oid = atooid(PQgetvalue(res, 0, PQfnumber(res, "reltype")));

	PQclear(res);
	resetPQExpBuffer(query);

	/* Check if there is a dependency entry in pg_depend from table to it's row type */
	appendPQExpBuffer(query,
					  "SELECT classid "
					  "FROM pg_catalog.pg_depend "
					  "WHERE deptype = 'i' "
					  "AND objid = '%u'::pg_catalog.oid "
					  "AND refobjid = '%u'::pg_catalog.oid "
					  "AND refclassid = 'pg_catalog.pg_type'::pg_catalog.regclass;",
					  tbinfo->dobj.catId.oid, pg_type_oid);

	res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
	ntups = PQntuples(res);

	PQclear(res);
	destroyPQExpBuffer(query);

	return ntups != 0;
}

/*
 * bbf_isTsqlMstvf:
 * Returns true if given function is T-SQL multi-statement
 * table valued function (MS-TVF), false otherwise.
 * A function is MS-TVF if it returns set (TABLE) and it's
 * return type is composite type.
 */
bool
bbf_isTsqlMstvf(Archive *fout, FuncInfo *finfo, char prokind, bool proretset)
{
	TypeInfo *rettype;

	if (prokind == PROKIND_PROCEDURE || !proretset)
		return false;

	rettype = findTypeByOid(finfo->prorettype);

	if (rettype->typtype == TYPTYPE_COMPOSITE &&
		strcmp(get_language_name(fout, finfo->lang), "pltsql") == 0)
		return true;

	return false;
}
