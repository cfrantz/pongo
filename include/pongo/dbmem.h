#ifndef PONGO_DBMEM_H
#define PONGO_DBMEM_H
#include <pongo/stdtypes.h>
#include <pongo/context.h>
#include <pongo/atomic.h>

#define GC_HASH_SZ 99991
#define GC_BUCKET_LEN 64
typedef struct {
	int len;
	void *ptr[];
} gcbucket_t;

#define GC_HASH_MALLOC_SZ(n) (sizeof(gchash_t) + n*sizeof(gcbucket_t*))
typedef struct {
	uint32_t hash_sz;
	gcbucket_t *bucket[];
} gchash_t;

typedef struct {
        uint32_t num, size;
} gccount_t;

typedef struct {
        gccount_t before;
        gccount_t after;
} gcstats_t;

#define NR_DB_CONTEXT 16
extern pgctx_t *dbctx[];

extern void dbmem_info(pgctx_t *ctx);
extern dbtype_t *_newkey(pgctx_t *ctx, dbtype_t *value);
extern pgctx_t *dbfile_open(const char *filename, uint32_t initsize);
extern void dbfile_close(pgctx_t *ctx);
extern void dbfile_sync(pgctx_t *ctx);
extern void dbfile_resize(pgctx_t *ctx);

extern void dblock(pgctx_t *ctx);
extern void dbunlock(pgctx_t *ctx);
extern void *dballoc(pgctx_t *ctx, int size);
extern void dbfree(pgctx_t *ctx, void *addr);
extern int dbsize(pgctx_t *ctx, void *addr);
extern int db_gc(pgctx_t *ctx, gcstats_t *stats);

static inline int synchronize(pgctx_t *ctx, int sync, uint64_t *ptr, void *oldval, void *newval)
{
    int ret;
    // Synchronize to disk to insure that all data structures
    // are in a consistent state
    if (sync) dbfile_sync(ctx);
    ret = cmpxchg(ptr, _offset(ctx, oldval), _offset(ctx, newval));
    // If the atomic exchange was successfull, synchronize again
    // to write the newly exchanged word to disk
    if (ret && sync) dbfile_sync(ctx);
    return ret;
}
#endif
