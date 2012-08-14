#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <limits.h>
#include <assert.h>
#include <pongo/dbtypes.h>
#include <pongo/bonsai.h>
#include <pongo/dbmem.h>
#include <pongo/misc.h>
#include <pongo/log.h>

dbtype_t *dbcache_new(pgctx_t *ctx, int cachesz, int retry)
{
    dbtype_t *cache;
    cache = dballoc(ctx, sizeof(dbcache_t));
    if (!cache) {
        log_error("Could not allocate cache: %d", cachesz);
        return NULL;
    }
    cache->type = Cache;
    cache->cache = 0;
    return cache;
}

int dbcache_recache(pgctx_t *ctx, int cachesz, int retry)
{
    return 0;
}

static dbtype_t *_dbcache_put(pgctx_t *ctx, dbtype_t *cache, dbtype_t *item)
{
    dbtype_t *node, *newnode;

    if (!cache) return item;
    assert(cache->type == Cache);
    do {
        node = _ptr(ctx, cache->cache);
        // We're really just using the cache here to keep a sorted
        // balanced atom tree, but set the value as well so we can
        // pretend its a k-v pair.
        newnode = bonsai_insert(ctx, node, item, item, 0);
        if (newnode == BONSAI_ERROR)
            return NULL;
    } while(!synchronize(ctx, 0, &cache->cache, node, newnode));
    return item;
}

dbtype_t *dbcache_put(pgctx_t *ctx, dbtype_t *item)
{
    return _dbcache_put(ctx, ctx->cache, item);
}

dbtype_t *dbcache_get_int(pgctx_t *ctx, dbtag_t type, int64_t ival)
{
    dbtype_t *cache = ctx->cache;
    if (!cache) return NULL;
    assert(cache->type == Cache);
    return bonsai_find_primitive(ctx, _ptr(ctx, cache->cache), type, &ival);
}

dbtype_t *dbcache_get_float(pgctx_t *ctx, dbtag_t type, double fval)
{
    dbtype_t *cache = ctx->cache;
    if (!cache) return NULL;
    assert(cache->type == Cache);
    return bonsai_find_primitive(ctx, _ptr(ctx, cache->cache), type, &fval);
}

dbtype_t *dbcache_get_str(pgctx_t *ctx, dbtag_t type, const char *sval, int len)
{
    dbtype_t *cache = ctx->cache;
    if (!cache) return NULL;
    assert(cache->type == Cache);
    return bonsai_find_primitive(ctx, _ptr(ctx, cache->cache), type, sval);
}

void dbcache_del(pgctx_t *ctx, dbtype_t *item)
{
    dbtype_t *cache = ctx->cache;
    dbtype_t *node, *newnode;

    if (!cache) return;
    if (item->type > String) {
        return;
    }
    assert(cache->type == Cache);

    do {
        // Read-Copy-Update loop for modify operations
        node = _ptr(ctx, cache->cache);
        newnode = bonsai_delete(ctx, node, item, NULL);
        if (newnode == BONSAI_ERROR)
            return ;
    } while(!synchronize(ctx, 0, &cache->cache, node, newnode));
}
/*
 * vim: ts=4 sts=4 sw=4 expandtab:
 */
