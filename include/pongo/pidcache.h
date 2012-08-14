#ifndef PONGO_PIDCACHE_H
#define PONGO_PIDCACHE_H
#include <pongo/context.h>
#include <pongo/dbtypes.h>

extern int pidcache_new(pgctx_t *ctx);
extern void pidcache_put(pgctx_t *ctx, void *localobj, dbtype_t *dbobj);
extern void pidcache_del(pgctx_t *ctx, void *localobj);
extern void pidcache_destroy(pgctx_t *ctx);

#endif
