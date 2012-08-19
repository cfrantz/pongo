#ifndef PONGO_CONTEXT_H
#define PONGO_CONTEXT_H

#include <pongo/mmfile.h>
#include <pongo/_dbtypes.h>

#define DBMEM_BUCKET_PAGE	8
#define DBMEM_BUCKETS		9
typedef struct _pgctx pgctx_t;
struct _pgctx {
	mmfile_t mm;
	// We keep two mb arrays: one sorted by ptr, the other by offset
	memblock_t **mb;
	memblock_t **mb_offset;
	int nr_mb;
	int last_mb[DBMEM_BUCKETS];
	int sync;
	dbroot_t *root;
	dbtype_t *cache;
	dbtype_t *data;
	// This pidcache is the pidcache for the currently running process.
	// The pidcache in root is the reference to the entire pidcache
	dbtype_t *pidcache;
	dbtype_t *(*newkey)(pgctx_t *ctx, dbtype_t *value);
};

// FIXME: this doesn't belong here
extern void dbfile_resize(pgctx_t *ctx);

/*
 * Return the memblock to which ptr belongs
 */
static inline memblock_t *__mb_ptr(pgctx_t *ctx, void *ptr)
{
	int imid, imin, imax;
	memblock_t *m;
	imin = 0;
	imax = ctx->nr_mb-1;
	while(imax >= imin) {
		imid = imin + (imax-imin)/2;
		m = ctx->mb[imid];
		if ((uint8_t*)ptr < (uint8_t*)m) {
			imax = imid-1;
		} else if((uint8_t*)ptr >=  (uint8_t*)m + m->mb_size) {
			imin = imid+1;
		} else {
			return m;
		}
	}
	assert(ptr == (void*)"Invalid Pointer");
	return NULL;
}

/*
 * Return the memblock to which offset belongs
 */
static inline memblock_t *__mb_offset(pgctx_t *ctx, uint64_t offset)
{
	int imid, imin, imax;
	memblock_t *m;
again:
	imin = 0;
	imax = ctx->nr_mb-1;
	while(imax >= imin) {
		imid = imax-imin / 2;
		m = ctx->mb_offset[imid];
		if (offset < m->mb_offset) {
			imax = imid-1;
		} else if(offset >=  m->mb_offset + m->mb_size) {
			imin = imid+1;
		} else {
			return m;
		}
	}
	// Check if the file has been resized
	if (offset < mm_size(&ctx->mm)) {
		dbfile_resize(ctx);
		goto again;
	}
	assert(offset == (long)"Invalid Offset");
	return NULL;
}

/*
 * Given a pointer in the mmap region, return its file offset
 */
static inline uint64_t __offset(pgctx_t *ctx, void *ptr)
{
	memblock_t *m = __mb_ptr(ctx, ptr);
	return m->mb_offset + ((uint8_t*)ptr - (uint8_t*)m);
}
static inline uint64_t _offset(pgctx_t *ctx, void *ptr)
{
	if (!ptr) return 0;
	return __offset(ctx, ptr);
}

/*
 * Given an offset in a mmaped file, return a pointer to the data
 */
static inline void *__ptr(pgctx_t *ctx, uint64_t offset)
{
	memblock_t *m = __mb_offset(ctx, offset);
	return (uint8_t*)m + offset - m->mb_offset;
}
static inline void *_ptr(pgctx_t *ctx, uint64_t offset)
{
	if (!offset) return NULL;
	return __ptr(ctx, offset);
}

#endif
