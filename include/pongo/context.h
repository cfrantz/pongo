#ifndef PONGO_CONTEXT_H
#define PONGO_CONTEXT_H

#include <pongo/mmfile.h>
#include <pongo/pmem.h>
#include <pongo/_dbtypes.h>

typedef struct _pgctx pgctx_t;
struct rcuhelper {
	unsigned len;
	dbtype_t addr[127];
};

struct _pgctx {
	mmfile_t mm;
	int sync;
	dbroot_t *root;
	dbtype_t cache;
	dbtype_t data;
	// This pidcache is the pidcache for the currently running process.
	// The pidcache in root is the reference to the entire pidcache
	dbtype_t pidcache;
	dbtype_t (*newkey)(pgctx_t *ctx, dbtype_t value);
	struct rcuhelper winner, loser;

};

/*
 * Given a pointer in the mmap region, return its file offset
 */
static inline uint64_t _offset(pgctx_t *ctx, void *ptr)
{
	if (!ptr) return 0;
	return __offset(&ctx->mm, ptr);
}

static inline dbtype_t dboffset(pgctx_t *ctx, void *ptr)
{
	dbtype_t ofs;
	// Total cheat here.  All addresses are 8-byte aligned,
	// so we can take advandate of the dbtype structure layout
	// and represent addresses naturally with no shifting or masking
	ofs.all = _offset(ctx, ptr);
	return ofs;
}

/*
 * Given an offset in a mmaped file, return a pointer to the data
 */
static inline void *_ptr(pgctx_t *ctx, uint64_t offset)
{
	if (!offset) return NULL;
	return __ptr(&ctx->mm, offset);
}

static inline void *dbptr(pgctx_t *ctx, dbtype_t offset)
{
	assert(isPtr(offset.type));
	return _ptr(ctx, offset.all);
}

static inline void rcureset(pgctx_t *ctx)
{
    ctx->loser.len = 0;
    ctx->winner.len = 0;
}

static inline void rculoser(pgctx_t *ctx)
{
    unsigned i;
    for(i=0; i<ctx->loser.len; i++) {
        pmem_sb_free(NULL, dbptr(ctx, ctx->loser.addr[i]));
    }
    rcureset(ctx);
}

static inline void rcuwinner(pgctx_t *ctx)
{
    unsigned i;
    for(i=0; i<ctx->winner.len; i++) {
        pmem_gc_suggest(dbptr(ctx, ctx->winner.addr[i]), 0xc4);
    }
    rcureset(ctx);
}



#endif
