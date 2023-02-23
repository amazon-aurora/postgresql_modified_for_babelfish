/*-------------------------------------------------------------------------
 *
 * compress_none.c
 *	 Routines for archivers to read or write an uncompressed stream.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	   src/bin/pg_dump/compress_none.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include <unistd.h>

#include "compress_none.h"
#include "pg_backup_utils.h"

/*----------------------
 * Compressor API
 *----------------------
 */

/*
 * Private routines
 */

static void
ReadDataFromArchiveNone(ArchiveHandle *AH, CompressorState *cs)
{
	size_t		cnt;
	char	   *buf;
	size_t		buflen;

	buf = pg_malloc(ZLIB_OUT_SIZE);
	buflen = ZLIB_OUT_SIZE;

	while ((cnt = cs->readF(AH, &buf, &buflen)))
	{
		ahwrite(buf, 1, cnt, AH);
	}

	free(buf);
}


static void
WriteDataToArchiveNone(ArchiveHandle *AH, CompressorState *cs,
					   const void *data, size_t dLen)
{
	cs->writeF(AH, data, dLen);
}

static void
EndCompressorNone(ArchiveHandle *AH, CompressorState *cs)
{
	/* no op */
}

/*
 * Public interface
 */

void
InitCompressorNone(CompressorState *cs,
				   const pg_compress_specification compression_spec)
{
	cs->readData = ReadDataFromArchiveNone;
	cs->writeData = WriteDataToArchiveNone;
	cs->end = EndCompressorNone;

	cs->compression_spec = compression_spec;
}


/*----------------------
 * Compress File API
 *----------------------
 */

/*
 * Private routines
 */

static size_t
read_none(void *ptr, size_t size, CompressFileHandle *CFH)
{
	FILE	   *fp = (FILE *) CFH->private_data;
	size_t		ret;

	if (size == 0)
		return 0;

	ret = fread(ptr, 1, size, fp);
	if (ret != size && !feof(fp))
		pg_fatal("could not read from input file: %s",
				 strerror(errno));

	return ret;
}

static size_t
write_none(const void *ptr, size_t size, CompressFileHandle *CFH)
{
	return fwrite(ptr, 1, size, (FILE *) CFH->private_data);
}

static const char *
get_error_none(CompressFileHandle *CFH)
{
	return strerror(errno);
}

static char *
gets_none(char *ptr, int size, CompressFileHandle *CFH)
{
	return fgets(ptr, size, (FILE *) CFH->private_data);
}

static int
getc_none(CompressFileHandle *CFH)
{
	FILE	   *fp = (FILE *) CFH->private_data;
	int			ret;

	ret = fgetc(fp);
	if (ret == EOF)
	{
		if (!feof(fp))
			pg_fatal("could not read from input file: %s", strerror(errno));
		else
			pg_fatal("could not read from input file: end of file");
	}

	return ret;
}

static int
close_none(CompressFileHandle *CFH)
{
	FILE	   *fp = (FILE *) CFH->private_data;
	int			ret = 0;

	CFH->private_data = NULL;

	if (fp)
		ret = fclose(fp);

	return ret;
}

static int
eof_none(CompressFileHandle *CFH)
{
	return feof((FILE *) CFH->private_data);
}

static int
open_none(const char *path, int fd, const char *mode, CompressFileHandle *CFH)
{
	Assert(CFH->private_data == NULL);

	if (fd >= 0)
		CFH->private_data = fdopen(dup(fd), mode);
	else
		CFH->private_data = fopen(path, mode);

	if (CFH->private_data == NULL)
		return 1;

	return 0;
}

static int
open_write_none(const char *path, const char *mode, CompressFileHandle *CFH)
{
	Assert(CFH->private_data == NULL);

	CFH->private_data = fopen(path, mode);
	if (CFH->private_data == NULL)
		return 1;

	return 0;
}

/*
 * Public interface
 */

void
InitCompressFileHandleNone(CompressFileHandle *CFH,
						   const pg_compress_specification compression_spec)
{
	CFH->open_func = open_none;
	CFH->open_write_func = open_write_none;
	CFH->read_func = read_none;
	CFH->write_func = write_none;
	CFH->gets_func = gets_none;
	CFH->getc_func = getc_none;
	CFH->close_func = close_none;
	CFH->eof_func = eof_none;
	CFH->get_error_func = get_error_none;

	CFH->private_data = NULL;
}
