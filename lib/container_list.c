#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <limits.h>
#include <assert.h>
#include <pongo/dbtypes.h>
#include <pongo/dbmem.h>
#include <pongo/misc.h>
#include <pongo/log.h>

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

int dblist_len(pgctx_t *ctx, dbtype_t *list)
{
	_list_t *_list;
	assert(list->type == List);
	_list = _ptr(ctx, list->list);
    return _list->len;
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
    dbfree(ctx, _list);
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
    dbfree(ctx, _list);
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
    dbfree(ctx, _list);
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
                return -1;
            }
            _newlist->item[i] = _offset(ctx, item);
        }
    } while(!synchronize(ctx, sync, &list->list, _list, _newlist));
    dbfree(ctx, _list);
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
            if (!remove && dbcmp(ctx, _ptr(ctx, _list->item[i]), item) == 0) {
                remove = 1;
            } else {
                _newlist->item[j++] = _list->item[i];
            }
        }
        if (!remove) {
            // No item was removed, so discard the copy (no one will
            // ever see it) and return an error
            return -1;
        }
    } while(!synchronize(ctx, sync, &list->list, _list, _newlist));
    dbfree(ctx, _list);
    return 0;
}

int dblist_contains(pgctx_t *ctx, dbtype_t *list, dbtype_t *item)
{
	_list_t *_list;
	int i;
	assert(list->type == List);

	_list = _ptr(ctx, list->list);
    for(i=0; i<_list->len; i++) {
        if (dbcmp(ctx, _ptr(ctx, _list->item[i]), item) == 0)
            return 1;
    }
    return 0;
}
// vim: ts=4 sts=4 sw=4 expandtab:
