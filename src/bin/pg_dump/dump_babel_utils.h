/*-------------------------------------------------------------------------
 *
 * Utility routines for babelfish objects
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/pg_dump/dump_babel_utils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DUMP_BABEL_UTILS_H
#define DUMP_BABEL_UTILS_H

#include "pg_dump.h"

extern void bbf_selectDumpableCast(CastInfo *cast);
extern void bbf_fixTableTypeDependency(Archive *fout, DumpableObject *func, DumpableObject *tabletype);
extern bool bbf_is_tsqltabletype(Archive *fout, const TableInfo *tbinfo);
extern bool bbf_is_tsql_mstvf(Archive *fout, FuncInfo *finfo, char prokind, bool proretset);

#endif
