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

int put_id_helper(pgctx_t *ctx, dbtype_t **key, dbtype_t *value)
{
    dbtype_t *id = _ptr(ctx, ctx->root->meta.id);
    if (!ctx->newkey) {
        // PUT_ID only works when a newkey function has been assigned
        return -1;
    }
    if (value->type == Object) {
        if (dbobject_getitem(ctx, value, id, key) < 0) {
            *key = ctx->newkey(ctx, value);
            dbobject_setitem(ctx, value, id, *key, 0);
        }
    } else if (value->type == Collection) {
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

int dbcollection_len(pgctx_t *ctx, dbtype_t *obj)
{
    return bonsai_size(_ptr(ctx, obj->obj));
}

int dbcollection_contains(pgctx_t *ctx, dbtype_t *obj, dbtype_t *key)
{
    return bonsai_find(ctx, _ptr(ctx, obj->obj), key, NULL) == 0;
}

dbtype_t *dbcollection_new(pgctx_t *ctx)
{
    dbtype_t *obj;
    obj = dballoc(ctx, sizeof(dbcollection_t));
    obj->type = Collection;
    obj->obj = 0;
    return obj;
}

int dbcollection_setitem(pgctx_t *ctx, dbtype_t *obj, dbtype_t *key, dbtype_t *value, int sync)
{
    dbtype_t *node, *newnode = NULL;
    assert(obj->type == Collection);

    if (sync & PUT_ID) {
        if (put_id_helper(ctx, &key, value) < 0)
            return -1;
    }

    do {
        // Read-Copy-Update loop for safe modify
        node = _ptr(ctx, obj->obj);
        dbfree(ctx, newnode);
        newnode = bonsai_insert(ctx, node, key, value, sync & SET_OR_FAIL);
        if (newnode == BONSAI_ERROR)
            return -1;
    } while(!synchronize(ctx, sync & SYNC_MASK, &obj->obj, node, newnode));
    dbfree(ctx, node);
    return 0;
}

int dbcollection_getitem(pgctx_t *ctx, dbtype_t *obj, dbtype_t *key, dbtype_t **value)
{
    return bonsai_find(ctx, _ptr(ctx, obj->obj), key, value);
}

int dbcollection_getstr(pgctx_t *ctx, dbtype_t *obj, const char *key, dbtype_t **value)
{
    *value = bonsai_find_primitive(ctx, _ptr(ctx, obj->obj), String, key);
    return 0;
}

int dbcollection_update(pgctx_t *ctx, dbtype_t *obj, int n, updatecb_t elem, void *user, int sync)
{
    int i;
    dbtype_t *key, *value;
    assert(obj->type == Collection);

    for(i=0; i<n; i++) {
        if (elem(ctx, i, &key, &value, user) < 0)
            return -1;
        dbcollection_setitem(ctx, obj, key, value, (i==n-1) ? sync : 0);
    }
    return 0;
}

int dbcollection_delitem(pgctx_t *ctx, dbtype_t *obj, dbtype_t *key, dbtype_t **value, int sync)
{
    dbtype_t *node, *newnode=NULL;
    assert(obj->type == Collection);
    // Read-Copy-Update loop for safe modify
    do {
        node = _ptr(ctx, obj->obj);
        dbfree(ctx, newnode);
        newnode = bonsai_delete(ctx, node, key, value);
        if (newnode == BONSAI_ERROR)
            return -1;
    } while(!synchronize(ctx, sync, &obj->obj, node, newnode));
    dbfree(ctx, node);
    return 0;
}

// vim: ts=4 sts=4 sw=4 expandtab:
