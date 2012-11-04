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

int db_multi(pgctx_t *ctx, dbtype_t obj, dbtype_t path, multi_t op, dbtype_t *value, int sync)
{
    int n, r;
    dbtype_t p, pp;
    dbval_t *objp;
    _list_t *list;

    if (dbtype(ctx, path) == List) {
        // Walk through "path" to the penultimate item
        pp.ptr = dbptr(ctx, path);
        list = dbptr(ctx, pp.ptr->list);
        for(n=0; n<list->len-1; n++) {
            dblist_getitem(ctx, path, n, &p);
            objp = dbptr(ctx, obj);
            if (objp->type == List) {
                p = dbstrtol(ctx, p);
                if (p.type == Error) {
                    return MULTI_ERR_PATH;
                }
                r = dblist_getitem(ctx, obj, p.val, &obj);
                if (r<0)
                    return MULTI_ERR_INDEX;
            } else if (objp->type == Object) {
                r = dbobject_getitem(ctx, obj, p, &obj);
                if (r<0)
                    return MULTI_ERR_KEY;
            } else if (objp->type == Collection) {
                r = dbcollection_getitem(ctx, obj, p, &obj);
                if (r<0)
                    return MULTI_ERR_KEY;
            } else {
                return MULTI_ERR_TYPE;
            }
        }

        // Now that obj is pointing at the second to last item, get the last
        // item in the path and get/set/del the item.
        dblist_getitem(ctx, path, n, &p);
    } else {
        p = path;
    }

    objp = dbptr(ctx, obj);
    if (objp->type == List) {
        p = dbstrtol(ctx, p);
        if (p.type == Error) {
            return MULTI_ERR_PATH;
        }
        if (op == multi_GET) {
            r = dblist_getitem(ctx, obj, p.val, value);
        } else if (op == multi_SET) {
            r = dblist_setitem(ctx, obj, p.val, *value, sync);
        } else if (op == multi_DEL) {
            r = dblist_delitem(ctx, obj, p.val, value, sync);
        } else {
            r = MULTI_ERR_CMD;
        }
        if (r == -1) r = MULTI_ERR_INDEX;
    } else if (objp->type == Object) {
        if (op == multi_GET) {
            r = dbobject_getitem(ctx, obj, p, value);
        } else if (op == multi_SET) {
            r = dbobject_setitem(ctx, obj, p, *value, sync);
            if (sync & PUT_ID) {
                // The PUT_ID option overwrites the value pointer on the way out
                // to point to the value's (possibly newly created) "_id" object.
                db_multi(ctx, *value, ctx->root->meta.id, multi_GET, value, 0);
            }
        } else if (op == multi_SET_OR_FAIL) {
            r = dbobject_setitem(ctx, obj, p, *value, sync | SET_OR_FAIL);
        } else if (op == multi_DEL) {
            r = dbobject_delitem(ctx, obj, p, value, sync);
        } else {
            r = MULTI_ERR_CMD;
        }
        if (r == -1) r = MULTI_ERR_KEY;
    } else if (objp->type == Collection) {
        if (op == multi_GET) {
            r = dbcollection_getitem(ctx, obj, p, value);
        } else if (op == multi_SET) {
            r = dbcollection_setitem(ctx, obj, p, *value, sync);
            if (sync & PUT_ID) {
                // The PUT_ID option overwrites the value pointer on the way out
                // to point to the value's (possibly newly created) "_id" object.
                db_multi(ctx, *value, ctx->root->meta.id, multi_GET, value, 0);
            }
        } else if (op == multi_SET_OR_FAIL) {
            r = dbcollection_setitem(ctx, obj, p, *value, sync | SET_OR_FAIL);
        } else if (op == multi_DEL) {
            r = dbcollection_delitem(ctx, obj, p, value, sync);
        } else {
            r = MULTI_ERR_CMD;
        }
        if (r == -1) r = MULTI_ERR_KEY;
    } else {
        r = MULTI_ERR_TYPE;
    }
    return r;
}

typedef struct {
    dbtype_t path;         // path list
    int n;                  // path index
    relop_t relop;          // relational operation
    dbtype_t value;        // The value against which relop is evaluated
    dbtype_t result;       // result object
    int r;                  // resutl boolean
} search_t;

static void search_helper(pgctx_t *ctx, dbtype_t node, void *user)
{
    search_t *search = (search_t*)user;
    
    node.ptr = dbptr(ctx, node);
    search->r = db_search(ctx, node.ptr->value,
            search->path, 
            search->n, 
            search->relop, 
            search->value, 
            search->result);
    if (search->r == 1 && search->n == 0)
        dbcollection_setitem(ctx, search->result, node.ptr->key, node.ptr->value, 0);
}
// Given a root object or collection, search all children for key (aka path) relop value.
// Put all results into the supplied result collection.
//
// The path expression may contain wildcards:
// db_search(ctx, coll, "foo.*.bar", db_EQ, 23, result);
//
// The wildcards will search over objects, collections or lists.
// Currently, a List may not be the first or last item in the search path:
//
// E.g. This will not work if the last item is a list
// db_search(ctx, coll, "foo.bar.*", db_EQ, 23, result);
//
// FIXME:  This function could probably be cleaned up quite a bit.
// FIXME:  This function does an exhaustive search.
int db_search(pgctx_t *ctx, dbtype_t obj, dbtype_t path, int n, relop_t relop, dbtype_t value, dbtype_t result)
{
    int cmp, i, r;
    dbtype_t p, x;
    dbval_t *objp;
    int otype;
    _list_t *list; _obj_t *o;
    search_t search;

    // If we've reached the last item, do the relational compare:
    if (n == dblist_len(ctx, path)) {
        cmp = dbcmp(ctx, obj, value);
        switch(relop) {
            case db_EQ:
                return cmp == 0;
            case db_NE:
                return cmp != 0;
            case db_LT:
                return cmp ==-1;
            case db_LE:
                return (cmp==-1 || cmp==0);
            case db_GT:
                return cmp == 1;
            case db_GE:
                return (cmp==1 || cmp==0);
        }
        return 0;
    }

    // Get the nth item from the path list
    if (n >= 0)
        dblist_getitem(ctx, path, n, &p);
    n++;

    otype = dbtype(ctx, obj);
    if (otype == Object) {
        objp = dbptr(ctx, obj);
        if (n==0 || dbcmp_primitive(ctx, p, String, "*") == 0) {
            // If we're at the beginning of the search or the search term is wildcard
            // For each item in "object"
            o = dbptr(ctx, objp->obj);
            for(i=0; i<o->len; i++) {
                x = o->item[i].value;
                r = db_search(ctx, x, path, n, relop, value, result);
                if (r==1) {
                    // When n==0, put found items into the result collection,
                    // otherwise return true
                    if (n==0)
                        dbcollection_setitem(ctx, result, o->item[i].key, x, 0);
                    else
                        return 1;
                }
            }
        } else {
            // Get the specific item and search it
            if (dbobject_getitem(ctx, obj, p, &x) == 0) {
                return db_search(ctx, x, path, n, relop, value, result);
            }
        }
    } else if (otype == Collection) {
        objp = dbptr(ctx, obj);
        if (n==0 || dbcmp_primitive(ctx, p, String, "*") == 0) {
            // If we're at the beginning of the search or the search term is wildcard
            // For each item in "collection"
            search.path = path;
            search.n = n;
            search.relop = relop;
            search.value = value;
            search.result = result;
            search.r = 0;
            bonsai_foreach(ctx, objp->obj, search_helper, &search);
            // For n==0, the search_helper will put the result objects into the
            // collection.  We may need to return true if we're into the path list 
            if (n && search.r == 1)
                return 1;
        } else {
            // Get the specific item and search it
            if (dbcollection_getitem(ctx, obj, p, &x) == 0) {
                return db_search(ctx, x, path, n, relop, value, result);
            }
        }
    } else if (otype == List) {
        objp = dbptr(ctx, obj);
        if (dbcmp_primitive(ctx, p, String, "*") == 0) {
            list = dbptr(ctx, objp->list);
            for(i=0; i<list->len; i++) {
                x = list->item[i];
                if (db_search(ctx, x, path, n, relop, value, result) == 1)
                    return 1;
            }
        } else {
            p = dbstrtol(ctx, p);
            if (p.type == Int) {
                i = p.val;
            } else {
                return 0;
            }
            if (dblist_getitem(ctx, obj, p.val, &x) == 0) {
                return db_search(ctx, x, path, n, relop, value, result);
            }
        }
    }
    return 0;
}

// vim: ts=4 sts=4 sw=4 expandtab:
