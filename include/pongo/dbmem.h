#ifndef PONGO_DBMEM_H
#define PONGO_DBMEM_H
#include <pongo/stdtypes.h>
#include <pongo/context.h>

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

extern void dbmem_info(pgctx_t *ctx);
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

#endif
