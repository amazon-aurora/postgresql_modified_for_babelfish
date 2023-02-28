/*-------------------------------------------------------------------------
 *
 * Utility routines for babelfish objects
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/pg_dump/dumpall_babel_utils.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "catalog/pg_class_d.h"
#include "catalog/pg_proc_d.h"
#include "catalog/pg_type_d.h"
#include "common/logging.h"
#include "dumpall_babel_utils.h"
#include "fe_utils/string_utils.h"
#include "pg_backup_db.h"
#include "pg_backup_utils.h"
#include "pg_backup.h"
#include "pg_dump.h"
#include "pqexpbuffer.h"

/* Babelfish virtual database to dump */
char *bbf_db_name = NULL;

/*
 * Returns query to fetch all Babelfish users of specified logical database
 * and all the logins.
 * drop_query decides whether the qeury is to DROP the roles.
 */
void
getBabelfishRolesQuery(PQExpBuffer buf, char *role_catalog, bool drop_query)
{
	if (!bbf_db_name)
		return;

	resetPQExpBuffer(buf);
	if (drop_query)
	{
		printfPQExpBuffer(buf,
						  "WITH bbf_roles AS "
						  "(SELECT rolname from sys.babelfish_authid_user_ext "
						  "WHERE database_name = '%s' "
						  "UNION SELECT rolname from sys.babelfish_authid_login_ext) "
						  "SELECT rc.rolname "
						  "FROM %s rc "
						  "INNER JOIN bbf_roles bc "
						  "ON rc.rolname = bc.rolname "
						  "WHERE rc.rolname !~ '^pg_' "
						  "ORDER BY 1", bbf_db_name, role_catalog);
	}
	else
	{
		printfPQExpBuffer(buf,
						  "WITH bbf_roles AS "
						  "(SELECT rolname from sys.babelfish_authid_user_ext "
						  "WHERE database_name = '%s' "
						  "UNION SELECT rolname from sys.babelfish_authid_login_ext) "
						  "SELECT oid, rc.rolname, rolsuper, rolinherit, "
						  "rolcreaterole, rolcreatedb, "
						  "rolcanlogin, rolconnlimit, rolpassword, "
						  "rolvaliduntil, rolreplication, rolbypassrls, "
						  "pg_catalog.shobj_description(oid, '%s') as rolcomment, "
						  "rc.rolname = current_user AS is_current_user "
						  "FROM %s rc "
						  "INNER JOIN bbf_roles bc "
						  "ON rc.rolname = bc.rolname "
						  "WHERE rc.rolname !~ '^pg_' "
						  "ORDER BY 2", bbf_db_name, role_catalog, role_catalog);
	}
}

/*
 * Returns query to fetch al the roles, members and grantors related
 * to Babelfish users and logins.
 */
void
getBabelfishRoleMembershipQuery(PQExpBuffer buf, char *role_catalog)
{
	if (!bbf_db_name)
		return;

	resetPQExpBuffer(buf);
	printfPQExpBuffer(buf, "WITH bbf_roles AS "
					  "(SELECT rc.oid, rc.rolname FROM %s rc "
					  "INNER JOIN sys.babelfish_authid_user_ext bc "
					  "ON rc.rolname = bc.rolname WHERE bc.database_name = '%s' "
					  "UNION SELECT rc.oid, rc.rolname FROM %s rc "
					  "INNER JOIN sys.babelfish_authid_login_ext bc "
					  "ON rc.rolname = bc.rolname) "
					  "SELECT ur.rolname AS roleid, "
					  "um.rolname AS member, "
					  "a.admin_option, "
					  "ug.rolname AS grantor "
					  "FROM pg_auth_members a "
					  "INNER JOIN bbf_roles ur on ur.oid = a.roleid "
					  "INNER JOIN bbf_roles um on um.oid = a.member "
					  "LEFT JOIN bbf_roles ug on ug.oid = a.grantor "
					  "WHERE NOT (ur.rolname ~ '^pg_' AND um.rolname ~ '^pg_')"
					  "ORDER BY 1,2,3", role_catalog, bbf_db_name, role_catalog);
}
