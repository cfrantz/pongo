#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <limits.h>
#include <assert.h>
#include <pongo/atomic.h>
#include <pongo/dbtypes.h>
#include <pongo/dbmem.h>
#include <pongo/misc.h>
#include <pongo/log.h>

static int dbobject_size(_obj_t *_obj, int n)
{
	int len;
	len = _obj ? _obj->len : 0;
	return sizeof(*_obj) + sizeof(_obj->item[0])*(len + n);
}

int dbobject_len(pgctx_t *ctx, dbtype_t obj)
{
    _obj_t *_obj;
    obj.ptr = dbptr(ctx, obj);
	assert(obj.ptr->type == Object);
	_obj = dbptr(ctx, obj.ptr->obj);
    return _obj ? _obj->len : 0;
}

dbtype_t dbobject_new(pgctx_t *ctx)
{
	dbtype_t obj;
    //_obj_t *_obj;
	obj.ptr = dballoc(ctx, sizeof(dbobject_t));
	obj.ptr->type = Object;
    // Its easier to implement all of the dict/object operations if the object
    // starts with an empty list of kv pairs
#if 0
    _obj = dballoc(ctx, sizeof(_obj));
    _obj->type = _InternalObj;
    _obj->len = 0;
    obj.ptr->obj = dboffset(ctx, _obj);
#else
    obj.ptr->obj = DBNULL;
#endif
	return dboffset(ctx, obj.ptr);
}

int dbobject_setitem(pgctx_t *ctx, dbtype_t obj, dbtype_t key, dbtype_t value, int sync)
{
	int i, j, sz, done, cmp, len;
    dbtype_t k;
    _obj_t *_obj, *_newobj = NULL;

    obj.ptr = dbptr(ctx, obj);
	assert(obj.ptr->type == Object);
    
    if (sync & PUT_ID) {
        // FIXME: put_id_helper is in container_coll.c.  Should be moved
        // somewhere nicer.
        extern int put_id_helper(pgctx_t *ctx, dbtype_t *key, dbtype_t value);
        if (put_id_helper(ctx, &key, value) < 0)
            return -1;
    }

    do {
        // Read-Copy-Update loop for safe modify
        _obj = dbptr(ctx, obj.ptr->obj);
        sz = dbobject_size(_obj, 1);
        dbfree(_newobj, 0xd1);

        len = _obj ? _obj->len : 0;
        _newobj = dballoc(ctx, sz);
        _newobj->type = _InternalObj;
        _newobj->len = len;

        for(done=i=j=0; i<len; i++,j++) {
            k = _obj->item[i].key;
            if (!done) {
                cmp = dbcmp(ctx, key, k);
                if (cmp < 0) {
                    _newobj->item[j].key = key;
                    _newobj->item[j].value = value;
                    _newobj->len++;
                    j++; done=1;
                    _newobj->item[j] = _obj->item[i];
                } else if (cmp == 0) {
                    if (sync & SET_OR_FAIL)
                        return -1;
                    _newobj->item[j].key = _obj->item[i].key;
                    _newobj->item[j].value = value;
                    done = 1;
                } else {
                    _newobj->item[j] = _obj->item[i];
                }
            } else {
                _newobj->item[j] = _obj->item[i];
            }
        }
        if (!done) {
            _newobj->item[j].key = key;
            _newobj->item[j].value = value;
            _newobj->len++;
        }
    } while(!synchronizep(ctx, sync & SYNC_MASK, &obj.ptr->obj, _obj, _newobj));
    dbfree(_obj, 0xd2);
    return 0;
}

_objitem_t *dbobject_find(pgctx_t *ctx, _obj_t *_obj, dbtype_t key)
{
    int imid, imin, imax, cmp;
    if (!_obj || _obj->len == 0)
        return NULL;

    imin = 0;
    imax = _obj->len-1;
    while(imax >= imin) {
        imid = imin + (imax-imin)/2;
        cmp = dbcmp(ctx, key, _obj->item[imid].key);
        if (cmp < 0) {
            imax = imid - 1;
        } else if (cmp > 0) {
            imin = imid + 1;
        } else {
            return &_obj->item[imid];
        }
    }
    return NULL;
}

int dbobject_getitem(pgctx_t *ctx, dbtype_t obj, dbtype_t key, dbtype_t *value)
{
    _obj_t *_obj;
    _objitem_t *item;

    obj.ptr = dbptr(ctx, obj);
    assert(obj.ptr->type == Object);

    _obj = dbptr(ctx, obj.ptr->obj);
    item = dbobject_find(ctx, _obj, key);
    if (item) {
        if (value) *value = item->value;
        return 0;
    }
    return -1;
}

int dbobject_contains(pgctx_t *ctx, dbtype_t obj, dbtype_t key)
{
    _obj_t *_obj;
    _objitem_t *item;

    obj.ptr = dbptr(ctx, obj);
    assert(obj.ptr->type == Object);

    _obj = dbptr(ctx, obj.ptr->obj);
    item = dbobject_find(ctx, _obj, key);
    return item != NULL;
}

int dbobject_getstr(pgctx_t *ctx, dbtype_t obj, const char *key, dbtype_t *value)
{
	int i, len;
    _obj_t *_obj;

    obj.ptr = dbptr(ctx, obj);
    assert(obj.ptr->type == Object);
    _obj = dbptr(ctx, obj.ptr->obj);
	if (_obj) {
        len = _obj->len;
		for(i=0; i<len; i++) {
			if (!dbcmp_primitive(ctx, _obj->item[i].key, String, key)) {
				if (value)
					*value = _obj->item[i].value;
				return 0;
			}
		}
	}
	return -1;
}

static int _partition(pgctx_t *ctx, _objitem_t *array, int left, int right, int pivot)
{
#define swap(x, y) do { \
        _objitem_t tmp = array[x]; \
        array[x] = array[y]; \
        array[y] = tmp; \
    } while(0)
    int i, stindex = left;
    dbtype_t pk = array[pivot].key;

    swap(pivot, right);
    for(i=left; i<right; i++) {
        if (dbcmp(ctx, array[i].key, pk) < 0) {
            swap(i, stindex);
            stindex++;
        }
    }
    swap(stindex, right);
    return stindex;
#undef swap
}

static void _quicksort(pgctx_t *ctx, _objitem_t *array, int left, int right)
{
    int pivot;
    if (left<right) {
        pivot = left + (right-left)/2;
        pivot = _partition(ctx, array, left, right, pivot);
        _quicksort(ctx, array, left, pivot-1);
        _quicksort(ctx, array, pivot+1, right);
    }
}

int dbobject_update(pgctx_t *ctx, dbtype_t obj, int n, updatecb_t elem, void *user, int sync)
{
	int i, sz, newlen;
    _obj_t *_obj, *_newobj = NULL;
    _objitem_t *item;
	dbtype_t key, value;

    obj.ptr = dbptr(ctx, obj);
	assert(obj.ptr->type == Object);
    // Read-Copy-Update loop for safe modify
    do {
        _obj = dbptr(ctx, obj.ptr->obj);
        sz = dbobject_size(_obj, n);
        dbfree(_newobj, 0xd3);
        _newobj = dballoc(ctx, sz);
        if (_obj)
            memcpy(_newobj, _obj, sizeof(_obj_t) + _obj->len*sizeof(_objitem_t));
        _newobj->type = _InternalObj;
        newlen = _newobj->len;
        for(i=0; i<n; i++) {
            if (elem(ctx, i, &key, &value, user) < 0)
                return -1;
            // Update replaces existing elements and/or adds new elements
            item = dbobject_find(ctx, _newobj, key);
            if (item) {
                // Replace an existing element
                item->value = value;
            } else {
                // Otherwise, append it to the item list and we'll sort later
                _newobj->item[newlen].key = key;
                _newobj->item[newlen].value = value;
                newlen++;
            }
        }
        // Now that all the items are added, use quicksort to put it in order
        _newobj->len = newlen;
        _quicksort(ctx, _newobj->item, 0, newlen-1);
    } while(!synchronizep(ctx, sync, &obj.ptr->obj, _obj, _newobj));
    dbfree(_obj, 0xd4);
    return 0;
}

int dbobject_delitem(pgctx_t *ctx, dbtype_t obj, dbtype_t key, dbtype_t *value, int sync)
{
	int i, j, sz, done;
    _obj_t *_obj, *_newobj = NULL;
    
    obj.ptr = dbptr(ctx, obj);
	assert(obj.ptr->type == Object);
    // Read-Copy-Update loop for safe modify
    do {
        _obj = dbptr(ctx, obj.ptr->obj);
        if (!_obj)
            return -1;
        sz = dbobject_size(_obj, 0);
        dbfree(_newobj, 0xd5);
        _newobj = dballoc(ctx, sz);
        _newobj->type = _InternalObj;
        _newobj->len = _obj->len - 1;
        for(done=i=j=0; i<_obj->len; i++) {
            if (dbcmp(ctx, _obj->item[i].key, key) == 0) {
                if (value)
                    *value = _obj->item[i].value;
                done = 1;
            } else {
                _newobj->item[j].key = _obj->item[i].key;
                _newobj->item[j].value = _obj->item[i].value;
                j++;
            }
        }
        if (!done) {
            // If the key didn't exist, discard the copy and return an error
            dbfree(_newobj, 0xd6);
            return -1;
        }
    } while(!synchronizep(ctx, sync, &obj.ptr->obj, _obj, _newobj));
    dbfree(_obj, 0xd7);
    return 0;
}

// vim: ts=4 sts=4 sw=4 expandtab:
