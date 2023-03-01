/*-------------------------------------------------------------------------
 *
 * compress_io.h
 *	 Interface to compress_io.c routines
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	   src/bin/pg_dump/compress_io.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __COMPRESS_IO__
#define __COMPRESS_IO__

#include "pg_backup_archiver.h"

/* Initial buffer sizes used in zlib compression. */
#define ZLIB_OUT_SIZE	4096
#define ZLIB_IN_SIZE	4096

extern char *supports_compression(const pg_compress_specification compression_spec);

/*
 * Prototype for callback function used in writeData()
 */
typedef void (*WriteFunc) (ArchiveHandle *AH, const char *buf, size_t len);

/*
 * Prototype for callback function used in readData()
 *
 * readData will call the read function repeatedly, until it returns 0 to signal
 * EOF. readData passes a buffer to read the data into in *buf, of length
 * *buflen. If that's not big enough for the callback function, it can free() it
 * and malloc() a new one, returning the new buffer and its size in *buf and
 * *buflen.
 *
 * Returns the number of bytes read into *buf, or 0 on EOF.
 */
typedef size_t (*ReadFunc) (ArchiveHandle *AH, char **buf, size_t *buflen);

typedef struct CompressorState CompressorState;
struct CompressorState
{
	/*
	 * Read all compressed data from the input stream (via readF) and print it
	 * out with ahwrite().
	 */
	void		(*readData) (ArchiveHandle *AH, CompressorState *cs);

	/*
	 * Compress and write data to the output stream (via writeF).
	 */
	void		(*writeData) (ArchiveHandle *AH, CompressorState *cs,
							  const void *data, size_t dLen);

	/*
	 * End compression and flush internal buffers if any.
	 */
	void		(*end) (ArchiveHandle *AH, CompressorState *cs);

	/*
	 * Callback function to read from an already processed input stream
	 */
	ReadFunc	readF;

	/*
	 * Callback function to write an already processed chunk of data.
	 */
	WriteFunc	writeF;

	/*
	 * Compression specification for this state.
	 */
	pg_compress_specification compression_spec;

	/*
	 * Private data to be used by the compressor.
	 */
	void	   *private_data;
};

extern CompressorState *AllocateCompressor(const pg_compress_specification compression_spec,
										   ReadFunc readF,
										   WriteFunc writeF);
extern void EndCompressor(ArchiveHandle *AH, CompressorState *cs);

/*
 * Compress File Handle
 */
typedef struct CompressFileHandle CompressFileHandle;

struct CompressFileHandle
{
	/*
	 * Open a file in mode.
	 *
	 * Pass either 'path' or 'fd' depending on whether a file path or a file
	 * descriptor is available. 'mode' can be one of 'r', 'rb', 'w', 'wb',
	 * 'a', and 'ab'. Requires an already initialized CompressFileHandle.
	 */
	int			(*open_func) (const char *path, int fd, const char *mode,
							  CompressFileHandle *CFH);

	/*
	 * Open a file for writing.
	 *
	 * 'mode' can be one of 'w', 'wb', 'a', and 'ab'. Requires an already
	 * initialized CompressFileHandle.
	 */
	int			(*open_write_func) (const char *path, const char *mode,
									CompressFileHandle *CFH);

	/*
	 * Read 'size' bytes of data from the file and store them into 'ptr'.
	 */
	size_t		(*read_func) (void *ptr, size_t size, CompressFileHandle *CFH);

	/*
	 * Write 'size' bytes of data into the file from 'ptr'.
	 */
	size_t		(*write_func) (const void *ptr, size_t size,
							   struct CompressFileHandle *CFH);

	/*
	 * Read at most size - 1 characters from the compress file handle into
	 * 's'.
	 *
	 * Stop if an EOF or a newline is found first. 's' is always null
	 * terminated and contains the newline if it was found.
	 */
	char	   *(*gets_func) (char *s, int size, CompressFileHandle *CFH);

	/*
	 * Read the next character from the compress file handle as 'unsigned
	 * char' cast into 'int'.
	 */
	int			(*getc_func) (CompressFileHandle *CFH);

	/*
	 * Test if EOF is reached in the compress file handle.
	 */
	int			(*eof_func) (CompressFileHandle *CFH);

	/*
	 * Close an open file handle.
	 */
	int			(*close_func) (CompressFileHandle *CFH);

	/*
	 * Get a pointer to a string that describes an error that occurred during a
	 * compress file handle operation.
	 */
	const char *(*get_error_func) (CompressFileHandle *CFH);

	/*
	 * Compression specification for this file handle.
	 */
	pg_compress_specification compression_spec;

	/*
	 * Private data to be used by the compressor.
	 */
	void	   *private_data;
};

/*
 * Initialize a compress file handle with the requested compression.
 */
extern CompressFileHandle *InitCompressFileHandle(const pg_compress_specification compression_spec);

/*
 * Initialize a compress file stream. Infer the compression algorithm
 * from 'path', either by examining its suffix or by appending the supported
 * suffixes in 'path'.
 */
extern CompressFileHandle *InitDiscoverCompressFileHandle(const char *path,
														  const char *mode);
extern int	EndCompressFileHandle(CompressFileHandle *CFH);
#endif
