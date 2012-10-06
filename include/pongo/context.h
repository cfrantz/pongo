#ifndef PONGO_CONTEXT_H
#define PONGO_CONTEXT_H

#include <pongo/mmfile.h>
#include <pongo/pmem.h>
#include <pongo/_dbtypes.h>

typedef struct _pgctx pgctx_t;
struct rcuhelper {
	unsigned len;
	dbtype_t *addr[127];
};

struct _pgctx {
	mmfile_t mm;
	int sync;
	dbroot_t *root;
	dbtype_t *cache;
	dbtype_t *data;
	// This pidcache is the pidcache for the currently running process.
	// The pidcache in root is the reference to the entire pidcache
	dbtype_t *pidcache;
	dbtype_t *(*newkey)(pgctx_t *ctx, dbtype_t *value);
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

/*
 * Given an offset in a mmaped file, return a pointer to the data
 */
static inline void *_ptr(pgctx_t *ctx, uint64_t offset)
{
	if (!offset) return NULL;
	return __ptr(&ctx->mm, offset);
}

static inline void rculoser(pgctx_t *ctx, void *addr)
{
    unsigned i;
    //pmem_gc_suggest(addr, 0xc1);
    //pmem_sb_free(NULL, addr);
    for(i=0; i<ctx->loser.len; i++) {
        pmem_sb_free(NULL, ctx->loser.addr[i]);
    }
    ctx->loser.len = 0;
    ctx->winner.len = 0;
}

static inline void rcuwinner(pgctx_t *ctx, void *addr)
{
    unsigned i;
    pmem_gc_suggest(addr, 0xc3);
    for(i=0; i<ctx->winner.len; i++) {
        pmem_gc_suggest(ctx->winner.addr[i], 0xc4);
    }
    ctx->loser.len = 0;
    ctx->winner.len = 0;
}



#endif
