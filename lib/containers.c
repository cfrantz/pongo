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

dbtype_t *dblist_new(pgctx_t *ctx)
{
	dbtype_t *list;
    _list_t *_list;
	list = dballoc(ctx, sizeof(dblist_t));
    if (!list) return NULL;
	list->type = List;
    // Its easier to implement all of the list operations if the object
    // starts with an empty list
    _list = dballoc(ctx, sizeof(_list));
    if (!_list) return NULL;
    _list->type = _InternalList;
    _list->len = 0;
    list->list = _offset(ctx, _list);
	return list;
}

int dblist_size(pgctx_t *ctx, dbtype_t *list, int n)
{
	int len;
	_list_t *_list;
	assert(list->type == List);
	_list = _ptr(ctx, list->list);
	len = _list ? _list->len : 0;
	return sizeof(*_list) + sizeof(_list->item[0])*(len + n);
}

int dblist_getitem(pgctx_t *ctx, dbtype_t *list, int n, dbtype_t **item)
{
	_list_t *_list;
	assert(list->type == List);
	_list = _ptr(ctx, list->list);
    // As in Python, negative list index means "from end of list"
    if (n<0) n += _list->len;
    if (n >= 0 && n < _list->len) {
        *item = _ptr(ctx, _list->item[n]);
        return 0;
    }
	return -1;
}

int dblist_setitem(pgctx_t *ctx, dbtype_t *list, int n, dbtype_t *item, int sync)
{
	_list_t *_list, *_newlist = NULL;
	int sz;
	assert(list->type == List);
    do {
        // Read-Copy-Update loop for modify operations
    	_list = _ptr(ctx, list->list);
        // As in Python, negative list index means "from end of list"
        if (n<0) n += _list->len;
        if (n < 0 || n >= _list->len)
	        return -1;

        sz = dblist_size(ctx, list, 0);
        dbfree(ctx, _newlist);
        _newlist = dballoc(ctx, sz);
        memcpy(_newlist, _list, sz);
        _newlist->item[n] = _offset(ctx, item);
    } while(!synchronize(ctx, sync, &list->list, _list, _newlist));
    return 0;
}

int dblist_delitem(pgctx_t *ctx, dbtype_t *list, int n, dbtype_t **item, int sync)
{
	_list_t *_list, *_newlist = NULL;
	int sz, i, j;
	assert(list->type == List);
    do {
        // Read-Copy-Update loop for modify operations
        _list = _ptr(ctx, list->list);
        // As in Python, negative list index means "from end of list"
        if (n<0) n += _list->len;
        if (n < 0 || n >= _list->len)
	        return -1;
        sz = dblist_size(ctx, list, -1);
        dbfree(ctx, _newlist);
        _newlist = dballoc(ctx, sz);
        _newlist->type = _InternalList;
        _newlist->len = _list->len - 1;
        for(i=j=0; i<_list->len; i++) {
            if (i==n) {
                *item = _ptr(ctx, _list->item[i]);
            } else {
                _newlist->item[j++] = _list->item[i];
            }
        }
    } while(!synchronize(ctx, sync, &list->list, _list, _newlist));
    return 0;
}

int dblist_insert(pgctx_t *ctx, dbtype_t *list, int n, dbtype_t *item, int sync)
{
	_list_t *_list, *_newlist = NULL;
	int sz, i, j;
	assert(list->type == List);
    // Read-Copy-Update loop for modify operations
    do {
        _list = _ptr(ctx, list->list);

        // INT_MAX is a magic value that means append
        if (n == INT_MAX) n = _list->len;
        // As in Python, negative list index means "from end of list"
        if (n<0) n += _list->len;
        if (n < 0 || n > _list->len)
            return -1;
        sz = dblist_size(ctx, list, 1);
        dbfree(ctx, _newlist);
        _newlist = dballoc(ctx, sz);
        _newlist->type = _InternalList;
        _newlist->len = _list->len + 1;
        for(i=j=0; i<_newlist->len; i++) {
            if (i == n) {
                _newlist->item[i] = _offset(ctx, item);
            } else {
                _newlist->item[i] = _list->item[j++];
            }
        }
    } while(!synchronize(ctx, sync, &list->list, _list, _newlist));
    return 0;
}

int dblist_append(pgctx_t *ctx, dbtype_t *list, dbtype_t *item, int sync)
{
    return dblist_insert(ctx, list, INT_MAX, item, sync);
}

int dblist_extend(pgctx_t *ctx, dbtype_t *list, int n, extendcb_t elem, void *user, int sync)
{
	_list_t *_list, *_newlist = NULL;
	int sz, i, j;
    dbtype_t *item;
	assert(list->type == List);

    // Read-Copy-Update loop for modify operations
    do {
	    _list = _ptr(ctx, list->list);
        sz = dblist_size(ctx, list, n);
        dbfree(ctx, _newlist);
        _newlist = dballoc(ctx, sz);
        _newlist->type = _InternalList;
        _newlist->len = _list->len + n;
        for(i=0; i<_list->len; i++) {
            _newlist->item[i] = _list->item[i];
        }
        for(j=0; j<n; j++, i++) {
            if (elem(ctx, j, &item, user) < 0) {
                dbfree(ctx, _newlist);
                return -1;
            }
            _newlist->item[i] = _offset(ctx, item);
        }
    } while(!synchronize(ctx, sync, &list->list, _list, _newlist));
    return 0;
}
int dblist_remove(pgctx_t *ctx, dbtype_t *list, dbtype_t *item, int sync)
{
	_list_t *_list, *_newlist = NULL;
	int sz, i, j, remove;
	assert(list->type == List);
    // Read-Copy-Update loop for modify operations
    do {
	    _list = _ptr(ctx, list->list);
        sz = dblist_size(ctx, list, 0);
        dbfree(ctx, _newlist);
        _newlist = dballoc(ctx, sz);
        _newlist->type = _InternalList;
        _newlist->len = _list->len - 1;
        remove = 0;
        for(i=j=0; i<_list->len; i++) {
            // Copy all items that don't match.  Only remove
            // one matching item
            if (!remove && dbeq(_ptr(ctx, _list->item[i]), item, 1)) {
                remove = 1;
            } else {
                _newlist->item[j++] = _list->item[i];
            }
        }
        if (!remove) {
            // No item was removed, so discard the copy (no one will
            // ever see it) and return an error
            dbfree(ctx, _newlist);
            return -1;
        }
    } while(!synchronize(ctx, sync, &list->list, _list, _newlist));
    return 0;
}

int dbobject_size(pgctx_t *ctx, dbtype_t *obj, int n)
{
	int len;
    _obj_t *_obj;
	assert(obj->type == Object);
	_obj = _ptr(ctx, obj->obj);
	len = _obj ? _obj->len : 0;
	return sizeof(*_obj) + sizeof(_obj->item[0])*(len + n);
}

dbtype_t *dbobject_new(pgctx_t *ctx)
{
	dbtype_t *obj;
    _obj_t *_obj;
	obj = dballoc(ctx, sizeof(dbobject_t));
	obj->type = Object;
    // Its easier to implement all of the dict/object operations if the object
    // starts with an empty list of kv pairs
    _obj = dballoc(ctx, 64);
    _obj->type = _InternalObj;
    _obj->len = 3;
    _obj->retry = 4;
    obj->obj = _offset(ctx, _obj);
	return obj;
}

_objitem_t *dbobject_find(pgctx_t *ctx, dbtype_t *obj, dbtype_t *key)
{
	int i;
    uint32_t hidx, hash;
	dbtype_t *k;
    _obj_t *_obj;
	assert(obj->type == Object);
    _obj = _ptr(ctx, obj->obj);

    // Find the requested key
    hash = dbhashval(key);
    for(i=0; i<_obj->retry; i++, hash=ror32(hash)+1) {
        hidx = hash % _obj->len;
        k = _ptr(ctx, _obj->item[hidx].key);
        if (dbeq(k, key, 0)) {
            return &_obj->item[hidx];
        }
    }
	return NULL;
}

_obj_t *dbobject_resize(pgctx_t *ctx, _obj_t *old)
{
    int sz = old->len;
    int i, j;
    uint32_t hash, hidx;
    _obj_t *obj;
    dbtype_t *k;

    for(;;) {
        // Given the current size, compute the next size
        if (sz < 4) sz=128;
        else if (sz < 8) sz=256;
        else if (sz < 16) sz=512;
        else if (sz < 32) sz=1024;
        else if (sz < 64) sz=2048;
        else if (sz < 128) sz=4096;
        else {
            // Add another page plus fudge factor (because sz will be a prime
            // less than a multiple of page size)
            sz = (sz*16 + 4096+256) & ~4095;
        }

        // Alloc the new object, then compute its length, as the first prime less than
        // the number of slots in the object
        obj = dballoc(ctx, sz);
        sz = sz/16;
        while(!is_prime(--sz));
        obj->type = _InternalObj;
        obj->len = sz;
        obj->retry = old->retry;

        // Copy the old object.
        for(i=0; i<old->len; i++) {
            if (old->item[i].key) {
                k = _ptr(ctx, old->item[i].key);
                hash = dbhashval(k);
                for(j=0; j<obj->retry; j++, hash=ror32(hash)+1) {
                    hidx = hash % obj->len;
                    if (obj->item[hidx].key == 0) {
                        obj->item[hidx].key = old->item[i].key;
                        obj->item[hidx].value = old->item[i].value;
                        break;
                    }
                }
                if (j == obj->retry) {
                    // Too many collisions copying object.  Resize and try again
                    continue;
                }
            }
        }
        break;
    }
    return obj;
}

int dbobject_setitem(pgctx_t *ctx, dbtype_t *obj, dbtype_t *key, dbtype_t *value, int sync)
{
	int i, done = 0;
    uint32_t hidx, hash;
    _obj_t *_obj, *_newobj = NULL;
    _objitem_t *item;
	assert(obj->type == Object);
    // Read-Copy-Update loop for safe modify
    do {
        _newobj = _obj = _ptr(ctx, obj->obj);

        // First check if the key is already present.
        item = dbobject_find(ctx, obj, key);
        if (item) {
            item->value = _offset(ctx, value);
            done = 1;
        }
        // If it wasn't, insert the key/value pair in an empty slot
        while(!done) {
            hash = dbhashval(key);
            for(i=0; i<_newobj->retry; i++, hash=ror32(hash)+1) {
                hidx = hash % _newobj->len;
                if (cmpxchg(&_newobj->item[hidx].value, 0, _offset(ctx, value))) {
                    _newobj->item[hidx].key = _offset(ctx, key);
                    done = 1;
                    break;
                }
            }
            if (i==_newobj->retry)
                _newobj = dbobject_resize(ctx, _newobj);
        }
    } while(!synchronize(ctx, sync, &obj->obj, _obj, _newobj));
    return 0;
}

int dbobject_getitem(pgctx_t *ctx, dbtype_t *obj, dbtype_t *key, dbtype_t **value)
{
    _objitem_t *item = dbobject_find(ctx, obj, key);
    if (item) {
        if (value) *value = _ptr(ctx, item->value);
        return 0;
    }
    return -1;
}

int dbobject_getstr(pgctx_t *ctx, dbtype_t *obj, const char *key, dbtype_t **value)
{
	int i;
	dbstring_t *k;
    _obj_t *_obj;
	assert(obj->type == Object);
    _obj = _ptr(ctx, obj->obj);
	if (_obj) {
		for(i=0; i<_obj->len; i++) {
			k = (dbstring_t*)_ptr(ctx, _obj->item[i].key);
			if (k && (k->type == String || k->type == ByteBuffer) && !strcmp((char*)k->sval, key)) {
				if (value)
				       	*value = _ptr(ctx, _obj->item[i].value);
				return 0;
			}
		}
	}
	return -1;
}

int dbobject_update(pgctx_t *ctx, dbtype_t *obj, int n, updatecb_t elem, void *user, int sync)
{
	int i;
	dbtype_t *key, *value;
	assert(obj->type == Object);

    for(i=0; i<n; i++) {
        if (elem(ctx, i, &key, &value, user) < 0)
            return -1;
        dbobject_setitem(ctx, obj, key, value, (i==n-1) ? sync : 0);
    }
    return 0;
}

int dbobject_delitem(pgctx_t *ctx, dbtype_t *obj, dbtype_t *key, dbtype_t **value, int sync)
{
    _obj_t *_obj;
    _objitem_t *item;
    uint64_t k;
	assert(obj->type == Object);
    // Read-Copy-Update loop for safe modify
    do {
        _obj = _ptr(ctx, obj->obj);
        item = dbobject_find(ctx, obj, key);
        if (!item)
            return -1;
        k = item->key;
        if (k && cmpxchg(&item->key, k, 0)) 
            item->value = 0;
    } while(!synchronize(ctx, sync, &obj->obj, _obj, _obj));
    return 0;
}

int dbobject_multi(pgctx_t *ctx, dbtype_t *obj, dbtype_t *path, multi_t op, dbtype_t **value, int sync)
{
    int n, i, r;
    dbtype_t *p;
    _list_t *list;
    assert(path->type == List);

    list = _ptr(ctx, path->list);
    // Walk through "path" to the penultimate item
    for(n=0; n<list->len-1; n++) {
        dblist_getitem(ctx, path, n, &p);
        if (obj->type == List) {
            if (p->type == Int) {
                i = p->ival;
            } else if ((p->type == String || p->type == ByteBuffer) && isdigit(p->sval[0])) {
                i = atoi((char*)p->sval);
            } else {
                return MULTI_ERR_PATH;
            }
            r = dblist_getitem(ctx, obj, i, &obj);
            if (r<0)
                return MULTI_ERR_INDEX;
        } else if (obj->type == Object) {
            r = dbobject_getitem(ctx, obj, p, &obj);
            if (r<0)
                return MULTI_ERR_KEY;
        } else {
            return MULTI_ERR_TYPE;
        }
    }

    // Now that obj is pointing at the second to last item, get the last
    // item in the path and get/set/del the item.
    dblist_getitem(ctx, path, n, &p);
    if (obj->type == List) {
        if (p->type == Int) {
            i = p->ival;
        } else if ((p->type == String || p->type == ByteBuffer) && isdigit(p->sval[0])) {
            i = atoi((char*)p->sval);
        } else {
            return MULTI_ERR_PATH;
        }
        if (op == multi_GET) {
            r = dblist_getitem(ctx, obj, i, value);
        } else if (op == multi_SET) {
            r = dblist_setitem(ctx, obj, i, *value, sync);
        } else if (op == multi_DEL) {
            r = dblist_delitem(ctx, obj, i, value, sync);
        } else {
            r = MULTI_ERR_CMD;
        }
        if (r == -1) r = MULTI_ERR_INDEX;
    } else if (obj->type == Object) {
        if (op == multi_GET) {
            r = dbobject_getitem(ctx, obj, p, value);
        } else if (op == multi_SET) {
            r = dbobject_setitem(ctx, obj, p, *value, sync);
        } else if (op == multi_DEL) {
            r = dbobject_delitem(ctx, obj, p, value, sync);
        } else {
            r = MULTI_ERR_CMD;
        }
        if (r == -1) r = MULTI_ERR_KEY;
    } else {
        r = MULTI_ERR_TYPE;
    }
    return r;
}

int dbobject_search(pgctx_t *ctx, dbtype_t *obj, dbtype_t *path, int n, relop_t relop, dbtype_t *value, dbtype_t *result)
{
    int cmp, i, r;
    dbtype_t *p, *x;
    _list_t *list; _obj_t *o;
    assert(path->type == List);

    list = _ptr(ctx, path->list);
    if (n == list->len) {
        cmp = dbcmp(obj, value);
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

    if (n >= 0)
        dblist_getitem(ctx, path, n, &p);
    n++;

    if (obj->type == Object) {
        if (n==0 || ((p->type == String || p->type == ByteBuffer) && !strcmp((char*)p->sval, "*"))) {
            o = _ptr(ctx, obj->obj);
            for(i=0; i<o->len; i++) {
                x = _ptr(ctx, o->item[i].value);
                if (!x) continue;
                r = dbobject_search(ctx, x, path, n, relop, value, result);
                if (r==1) {
                    if (n==0)
                        dbobject_setitem(ctx, result, _ptr(ctx, o->item[i].key), x, 0);
                    else
                        return 1;
                }
            }
        } else {
            if (dbobject_getitem(ctx, obj, p, &x) == 0) {
                return dbobject_search(ctx, x, path, n, relop, value, result);
            }
        }
    } else if (obj->type == List) {
        if ((p->type == String || p->type == ByteBuffer) && !strcmp((char*)p->sval, "*")) {
            list = _ptr(ctx, obj->list);
            for(i=0; i<list->len; i++) {
                x = _ptr(ctx, list->item[i]);
                if (dbobject_search(ctx, x, path, n, relop, value, result) == 1)
                    return 1;
            }
        } else {
            if (p->type == Int) {
                i = p->ival;
            } else if ((p->type == String || p->type == ByteBuffer) && isdigit(p->sval[0])) {
                i = atoi((char*)p->sval);
            } else {
                return 0;
            }
            if (dblist_getitem(ctx, obj, p->ival, &x) == 0) {
                return dbobject_search(ctx, x, path, n, relop, value, result);
            }
        }
    }
    return 0;
}

void dbtypes_free(pgctx_t *ctx, dbtype_t *a)
{
	int i;
    _list_t *_list;
    _obj_t *_obj;
	if (!a) return;
	//log_debug("freeing %d at %p\n", a->type, a);
	switch(a->type) {
		case List:
            _list = _ptr(ctx, a->list);
			dbfree(ctx, a);
			for(i=0; i<_list->len; i++) {
				dbtypes_free(ctx, (dbtype_t*)_ptr(ctx, _list->item[i]));
			}
			break;
		case Object:
            _obj = _ptr(ctx, a->obj);
			dbfree(ctx, a);
			for(i=0; i<_obj->len; i++) {
				dbtypes_free(ctx, (dbtype_t*)_ptr(ctx, _obj->item[i].key));
				dbtypes_free(ctx, (dbtype_t*)_ptr(ctx, _obj->item[i].value));
			}
			break;
		default:
			dbfree(ctx, a);
	}
}

/*
 * A list of primes near values of 2**n.  Used for hashtable sizing for the cache.
 */
static const int prime_mod [] =
{
    1,
    2,
    3,
    7,
    13,
    31,
    61,
    127,
    251,
    509,
    1021,
    2039,
    4093,
    8191,
    16381,
    32749,
    65521,
    131071,
    262139,
    524287,
    1048573,
    2097143,
    4194301,
    8388593,
    16777213,
    33554393,
    67108859,
    134217689,
    268435399,
    536870909,
    1073741789,
    2147483647,
};


dbtype_t *dbcache_new(pgctx_t *ctx, int cachesz, int retry)
{
    dbtype_t *cache;
    _cache_t *_cache;

    if (cachesz == 0) {
        cachesz = 8191;
    } else if (cachesz < 32) {
        cachesz = prime_mod[cachesz];
    }
    if (retry == 0) retry = 16;

    cache = dballoc(ctx, sizeof(dbcache_t));
    _cache = dballoc(ctx, sizeof(_cache_t) + cachesz*sizeof(uint64_t));
    if (!cache) {
        log_error("Could not allocate cache: %d", cachesz);
        return NULL;
    }
    cache->type = Cache;
    cache->cache = _offset(ctx, _cache);
    _cache->len = cachesz;
    _cache->retry = retry;

    return cache;
}

int dbcache_recache(pgctx_t *ctx, int cachesz, int retry)
{
    _cache_t *_cache, *_newcache;
    uint64_t offset;
    uint32_t hash;
    int i, j;

    _cache = _ptr(ctx, ctx->cache->cache);
    if (cachesz == 0) {
        cachesz = 8191;
    } else if (cachesz < 32) {
        cachesz = prime_mod[cachesz];
    }
    if (retry == 0) retry = 16;

    // If the cache is the same size, then don't do anything
    if (cachesz == _cache->len && retry == _cache->retry) {
        return -0;
    }

    // Otherwise, do a read-copy-update copying the cache to the new
    // array
    do {
        _cache = _ptr(ctx, ctx->cache->cache);
        _newcache = dballoc(ctx, sizeof(_cache_t) + cachesz*sizeof(uint64_t));
        if (!_newcache) {
            log_error("Could not allocate new cache: %d", cachesz);
            return -1;
        }
        _newcache->len = cachesz;
        _newcache->retry = retry;

        for(i=0; i<_cache->len; i++) {
            offset = _cache->item[i];
            if (!offset)
                continue;

            hash = dbhashval(_ptr(ctx, offset));
            for(j=0; j<_newcache->retry; j++, hash=ror32(hash)+1) {
                if (cmpxchg(&_newcache->item[hash % _newcache->len], 0, offset))
                    break;
            }
            if (j == _newcache->retry) {
                log_error("Collision resizing hash!");
                return -1;
            }
        }
    } while(!synchronize(ctx, 0, &ctx->cache->cache, _cache, _newcache));
    return 0;
}

static dbtype_t *_dbcache_put(pgctx_t *ctx, dbtype_t *cache, dbtype_t *item)
{
    int i;
    dbtype_t *citem;
    _cache_t *_cache;
    uint32_t hash;
    uint32_t hashval;
    uint64_t offset;

    if (!cache) return item;
    assert(cache->type == Cache);
    _cache = _ptr(ctx, cache->cache);

    hashval = hash = dbhashval(item);
    // If an item has an invalid hashval, then just return the item.
    if (hashval == -1UL)
        return item;

    for(i=0; i<_cache->retry; i++, hash=ror32(hash)+1) {
        citem = _ptr(ctx, _cache->item[hash % _cache->len]);
        if (dbeq(citem, item, 0))
            return citem;
    }
    offset = _offset(ctx, item);
    hash = hashval;
    for(i=0; i<_cache->retry; i++, hash=ror32(hash)+1) {
        if (cmpxchg(&_cache->item[hash % _cache->len], 0, offset))
            return item;
    }
    log_info("Cache full for hash = %08lx", hashval);
    if (item->type == String || item->type==ByteBuffer)
        log_info("    sval=%s", item->sval);
    if (item->type == Int)
        log_info("    ival=%lld", item->ival);
    return item;
}

dbtype_t *dbcache_put(pgctx_t *ctx, dbtype_t *item)
{
    return _dbcache_put(ctx, ctx->cache, item);
}

dbtype_t *dbcache_get_int(pgctx_t *ctx, dbtag_t type, int64_t ival)
{
    int i;
    dbtype_t *cache = ctx->cache;
    _cache_t *_cache;
    dbtype_t *p;
    uint32_t hash;

    if (!cache) return NULL;
    assert(cache->type == Cache);
    _cache = _ptr(ctx, cache->cache);
    
    hash = (int32_t)ival ^ ival>>32;
    if (hash == -1UL) hash = -2UL;

    for(i=0; i<_cache->retry; i++, hash=ror32(hash)+1) {
        p = _ptr(ctx, _cache->item[hash % _cache->len]);
        if (p && p->type == type && p->ival == ival)
            return p;
    }
    return NULL;
}

dbtype_t *dbcache_get_float(pgctx_t *ctx, dbtag_t type, double fval)
{
    int i;
    dbtype_t *cache = ctx->cache;
    _cache_t *_cache;
    dbtype_t *p;
    uint32_t hash;
    union {
        int64_t i;
        double f;
    } val;

    if (!cache) return NULL;
    assert(cache->type == Cache);
    _cache = _ptr(ctx, cache->cache);
    
    val.f = fval;
    hash = (int32_t)val.i ^ val.i>>32;
    if (hash == -1UL) hash = -2UL;

    for(i=0; i<_cache->retry; i++, hash=ror32(hash)+1) {
        p = _ptr(ctx, _cache->item[hash % _cache->len]);
        if (p && p->type == type && p->fval == fval)
            return p;
    }
    return NULL;
}

dbtype_t *dbcache_get_str(pgctx_t *ctx, dbtag_t type, const char *sval, int len)
{
    int i;
    dbtype_t *cache = ctx->cache;
    _cache_t *_cache;
    dbtype_t *p;
    uint32_t hash = 0;

    if (!cache) return NULL;
    assert(cache->type == Cache);
    _cache = _ptr(ctx, cache->cache);
    
    hash = hash_x31((const uint8_t*)sval, len);

    for(i=0; i<_cache->retry; i++, hash=ror32(hash)+1) {
        p = _ptr(ctx, _cache->item[hash % _cache->len]);
        if (p && p->type == type && !strcmp((char*)p->sval, sval))
            return p;
    }
    return NULL;
}

void dbcache_del(pgctx_t *ctx, dbtype_t *item)
{
    int i;
    dbtype_t *cache = ctx->cache;
    _cache_t *_cache;
    uint32_t hash;
    uint64_t offset;

    if (!cache) return;
    assert(cache->type == Cache);
    _cache = _ptr(ctx, cache->cache);

    hash = dbhashval(item);
    // If the item has an invalid hashval, it's not in the cache
    if (hash == -1UL)
        return;
    offset = _offset(ctx, item);
#if 0
    log_debug("Freeing item at %llx %s", offset,
            (item->type == String || item->type == ByteBuffer) ? (char*)item->sval : "");
#endif
    for(i=0; i<_cache->retry; i++, hash=ror32(hash)+1) {
        if (cmpxchg(&_cache->item[hash % _cache->len], offset, 0))
            return;
    }
    return;
}
/*
 * vim: ts=4 sts=4 sw=4 expandtab:
 */
