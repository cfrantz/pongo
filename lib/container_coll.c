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

int put_id_helper(pgctx_t *ctx, dbtype_t *key, dbtype_t value)
{
    dbtype_t id = ctx->root->meta.id;
    dbval_t *ptr;
    if (!ctx->newkey) {
        // PUT_ID only works when a newkey function has been assigned
        return -1;
    }
    ptr = dbptr(ctx, value);
    if (ptr->type == Object) {
        if (dbobject_getitem(ctx, value, id, key) < 0) {
            *key = ctx->newkey(ctx, value);
            dbobject_setitem(ctx, value, id, *key, 0);
        }
    } else if (ptr->type == Collection) {
        if (dbcollection_getitem(ctx, value, id, key) < 0) {
            *key = ctx->newkey(ctx, value);
            dbcollection_setitem(ctx, value, id, *key, 0);
        }
    } else {
        // PUT_ID only works when value is a Object or Collection
        return -1;
    }
    return 0;
}

int dbcollection_len(pgctx_t *ctx, dbtype_t obj)
{
    obj.ptr = dbptr(ctx, obj);
    return bonsai_size(ctx, obj.ptr->obj);
}

int dbcollection_contains(pgctx_t *ctx, dbtype_t obj, dbtype_t key)
{
    obj.ptr = dbptr(ctx, obj);
    return bonsai_find(ctx, obj.ptr->obj, key, NULL) == 0;
}

dbtype_t dbcollection_new(pgctx_t *ctx)
{
    dbtype_t obj;
    obj.ptr = dballoc(ctx, sizeof(dbcollection_t));
    obj.ptr->type = Collection;
    obj.ptr->obj = DBNULL;
    return dboffset(ctx, obj.ptr);
}

int dbcollection_setitem(pgctx_t *ctx, dbtype_t obj, dbtype_t key, dbtype_t value, int sync)
{
    dbtype_t node, newnode;

    obj.ptr = dbptr(ctx, obj);
    assert(obj.ptr->type == Collection);

    if (sync & PUT_ID) {
        if (put_id_helper(ctx, &key, value) < 0)
            return -1;
    }

    assert(ctx->winner.len == 0);
    assert(ctx->loser.len == 0);
    do {
        // Read-Copy-Update loop for safe modify
        node = obj.ptr->obj;
        rculoser(ctx);
        newnode = bonsai_insert(ctx, node, key, value, sync & SET_OR_FAIL);
        if (newnode.type == Error) {
            rcureset(ctx);
            return -1;
        }
    } while(!synchronize(ctx, sync & SYNC_MASK, &obj.ptr->obj, node, newnode));
    rcuwinner(ctx);
    return 0;
}

int dbcollection_getitem(pgctx_t *ctx, dbtype_t obj, dbtype_t key, dbtype_t *value)
{
    obj.ptr = dbptr(ctx, obj);
    return bonsai_find(ctx, obj.ptr->obj, key, value);
}

int dbcollection_getstr(pgctx_t *ctx, dbtype_t obj, const char *key, dbtype_t *value)
{
    obj.ptr = dbptr(ctx, obj);
    *value = bonsai_find_primitive(ctx, obj.ptr->obj, String, key);
    return 0;
}

int dbcollection_update(pgctx_t *ctx, dbtype_t obj, int n, updatecb_t elem, void *user, int sync)
{
    int i;
    dbtype_t key, value;

    for(i=0; i<n; i++) {
        if (elem(ctx, i, &key, &value, user) < 0)
            return -1;
        dbcollection_setitem(ctx, obj, key, value, (i==n-1) ? sync : 0);
    }
    return 0;
}

int dbcollection_delitem(pgctx_t *ctx, dbtype_t obj, dbtype_t key, dbtype_t *value, int sync)
{
    dbtype_t node, newnode;

    obj.ptr = dbptr(ctx, obj);
    assert(obj.ptr->type == Collection);
    assert(ctx->winner.len == 0);
    assert(ctx->loser.len == 0);
    // Read-Copy-Update loop for safe modify
    do {
        node = obj.ptr->obj;
        rculoser(ctx);
        newnode = bonsai_delete(ctx, node, key, value);
        if (newnode.type == Error) {
            rcureset(ctx);
            return -1;
        }
    } while(!synchronize(ctx, sync, &obj.ptr->obj, node, newnode));
    rcuwinner(ctx);
    return 0;
}

// vim: ts=4 sts=4 sw=4 expandtab:
