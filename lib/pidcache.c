#include <stdlib.h>
#include <pongo/context.h>
#include <pongo/dbtypes.h>
#include <pongo/atomic.h>
#include <pongo/misc.h>
#include <pongo/log.h>

int pidcache_new(pgctx_t *ctx)
{
	dbtype_t pc;
	dbtype_t root_pc;
	dbtype_t pid;
    dbval_t *pcp;
    int _pid;

    root_pc = ctx->root->pidcache;
    _pid = getpid();
	pid = dbint_new(ctx, _pid);
	pc = dbcollection_new(ctx, 0);

	if (dbcollection_setitem(ctx, root_pc, pid, pc, SET_OR_FAIL) < 0) {
        log_error("pid=%d: pidcache already exists", _pid);
        dbcollection_getitem(ctx, root_pc, pid, &pc);
    }

    pcp = dbptr(ctx, pc);
    atomic_inc(&pcp->refcnt);
    ctx->pidcache = pc;
    return _pid;
}

void pidcache_put(pgctx_t *ctx, void *localobj, dbtype_t dbobj)
{
    dbtype_t key;
    // make sure the pidcache is valid
    // also make sure we aren't trying to put the pidcache into
    // the pidcache.  The GC will die horribly if there is a cycle.
    if (ctx->pidcache.all && ctx->pidcache.all != dbobj.all) {
        key = dbint_new(ctx, (unsigned long)localobj);
        dbcollection_setitem(ctx, ctx->pidcache, key, dbobj, 0);
    }
}

void pidcache_del(pgctx_t *ctx, void *localobj)
{
    dbtype_t key;
    if (ctx->pidcache.all) {
        key = dbint_new(ctx, (unsigned long)localobj);
        dbcollection_delitem(ctx, ctx->pidcache, key, NULL, 0);
    }
}

void pidcache_destroy(pgctx_t *ctx)
{
	dbtype_t root_pc;
	dbtype_t pid;
    dbval_t *pcp;
    int _pid;

    if (!ctx->pidcache.all)
        return;

    root_pc = ctx->root->pidcache;
    _pid = getpid();
	pid = dbint_new(ctx, _pid);

    pcp = dbptr(ctx, ctx->pidcache);
    if (atomic_dec(&pcp->refcnt) == 0) {
        dbcollection_delitem(ctx, root_pc, pid, NULL, 0);
        ctx->pidcache = DBNULL;
    } else {
        log_error("pid=%d: pidcache not destroyed (refcnt=%d)", _pid, pcp->refcnt);
    }
}

// vim: ts=4 sts=4 sw=4 expandtab:
