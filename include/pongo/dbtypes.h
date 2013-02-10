#ifndef PONGO_DBTYPES_H
#define PONGO_DBTYPES_H

#include <time.h>
#include <pongo/context.h>

extern const dbtype_t DBNULL;

#define NOSYNC		0x0000
#define SYNC		0x0001
#define SYNC_MASK	0x000F
#define SET_OR_FAIL	0x0010		// only valid for obj/coll _setitem
#define PUT_ID		0x0020		// only valid for obj/coll _setitem

#define MULTI_ERR_PATH  -1
#define MULTI_ERR_KEY   -2
#define MULTI_ERR_INDEX -3
#define MULTI_ERR_TYPE  -4
#define MULTI_ERR_CMD   -5

typedef enum {
	db_EQ,
	db_NE,
	db_LT,
	db_LE,
	db_GT,
	db_GE,
} relop_t;

typedef enum {
	multi_GET,
	multi_SET,
	multi_SET_OR_FAIL,
	multi_DEL,
} multi_t;

static inline dbtag_t dbtype(pgctx_t *ctx, dbtype_t x)
{
	dbtag_t type;
	type = x.type;
	if (isPtr(type)) {
		x.ptr = dbptr(ctx, x);
		if (x.ptr) type = x.ptr->type;
	}
	return type;
}

// In dbtypes.c
extern dbtype_t dbboolean_new(pgctx_t *ctx, unsigned val);
extern dbtype_t dbint_new(pgctx_t *ctx, int64_t val);
extern dbtype_t dbstrtol(pgctx_t *ctx, dbtype_t s);
extern dbtype_t dbfloat_new(pgctx_t *ctx, double val);
extern dbtype_t dbstring_new(pgctx_t *ctx, const char *val, int len);
extern dbtype_t dbbuffer_new(pgctx_t *ctx, const char *val, int len);
extern dbtype_t dbuuid_new(pgctx_t *ctx, uint8_t *val);
extern dbtype_t dbuuid_new_fromstring(pgctx_t *ctx, const char *val);
extern dbtype_t dbtime_new(pgctx_t *ctx, int64_t val);
extern dbtype_t dbtime_newtm(pgctx_t *ctx, struct tm *tm, long usec);
extern dbtype_t dbtime_now(pgctx_t *ctx);

extern int dbcmp(pgctx_t *ctx, dbtype_t a, dbtype_t b);
extern int dbcmp_primitive(pgctx_t *ctx, dbtype_t a, dbtag_t btype, const void *b);
extern char *dbprint(pgctx_t *ctx, dbtype_t a, char *buf, int n);

// Callback types for _extend and _update
typedef int (*extendcb_t)(pgctx_t *ctx, int i, dbtype_t *item, void *user);
typedef int (*updatecb_t)(pgctx_t *ctx, int i, dbtype_t *key, dbtype_t *value, void *user);

// In container_list.c
extern dbtype_t dblist_new(pgctx_t *ctx);
extern int dblist_len(pgctx_t *ctx, dbtype_t list);
extern int dblist_getitem(pgctx_t *ctx, dbtype_t list, int n, dbtype_t *item);
extern int dblist_setitem(pgctx_t *ctx, dbtype_t list, int n, dbtype_t item, int sync);
extern int dblist_delitem(pgctx_t *ctx, dbtype_t list, int n, dbtype_t *item, int sync);
extern int dblist_insert(pgctx_t *ctx, dbtype_t list, int n, dbtype_t item, int sync);
extern int dblist_append(pgctx_t *ctx, dbtype_t list, dbtype_t item, int sync);
extern int dblist_extend(pgctx_t *ctx, dbtype_t list, int n, extendcb_t elem, void *user, int sync);
extern int dblist_remove(pgctx_t *ctx, dbtype_t list, dbtype_t item, int sync);
extern int dblist_contains(pgctx_t *ctx, dbtype_t list, dbtype_t item);

// In container_obj.c
extern dbtype_t dbobject_new(pgctx_t *ctx);
extern int dbobject_len(pgctx_t *ctx, dbtype_t obj);
extern int dbobject_contains(pgctx_t *ctx, dbtype_t obj, dbtype_t key);
extern int dbobject_getitem(pgctx_t *ctx, dbtype_t obj, dbtype_t key, dbtype_t *value);
extern int dbobject_getstr(pgctx_t *ctx, dbtype_t obj, const char *key, dbtype_t *value);
extern int dbobject_setitem(pgctx_t *ctx, dbtype_t obj, dbtype_t key, dbtype_t value, int sync);
extern int dbobject_delitem(pgctx_t *ctx, dbtype_t obj, dbtype_t key, dbtype_t *value, int sync);
extern int dbobject_update(pgctx_t *ctx, dbtype_t obj, int n, updatecb_t elem, void *user, int sync);

// In container_coll.c
extern dbtype_t dbcollection_new(pgctx_t *ctx, int multi);
extern int dbcollection_len(pgctx_t *ctx, dbtype_t obj);
extern int dbcollection_contains(pgctx_t *ctx, dbtype_t obj, dbtype_t key);
extern int dbcollection_getitem(pgctx_t *ctx, dbtype_t obj, dbtype_t key, dbtype_t *value);
extern int dbcollection_getnode(pgctx_t *ctx, dbtype_t obj, dbtype_t key, dbtype_t *value);
extern int dbcollection_getstr(pgctx_t *ctx, dbtype_t obj, const char *key, dbtype_t *value);
extern int dbcollection_setitem(pgctx_t *ctx, dbtype_t obj, dbtype_t key, dbtype_t value, int sync);
extern int dbcollection_delitem(pgctx_t *ctx, dbtype_t obj, dbtype_t key, dbtype_t *value, int sync);
extern int dbcollection_update(pgctx_t *ctx, dbtype_t obj, int n, updatecb_t elem, void *user, int sync);


// In container_ops.c
extern int db_search(pgctx_t *ctx, dbtype_t obj, dbtype_t path, int n, relop_t relop, dbtype_t value, dbtype_t result);
extern int db_multi(pgctx_t *ctx, dbtype_t obj, dbtype_t path, multi_t op, dbtype_t *value, int sync);

// In container_cache.c
#if 0
extern dbtype_t *dbcache_new(pgctx_t *ctx, int cachesz, int retry);
extern int dbcache_recache(pgctx_t *ctx, int cachesz, int retry);
extern dbtype_t *dbcache_put(pgctx_t *ctx, dbtype_t *obj);
extern dbtype_t *dbcache_get_int(pgctx_t *ctx, dbtag_t type, int64_t ival);
extern dbtype_t *dbcache_get_float(pgctx_t *ctx, dbtag_t type, double fval);
extern dbtype_t *dbcache_get_str(pgctx_t *ctx, dbtag_t type, const char *sval, int len);
extern void dbcache_del(pgctx_t *ctx, dbtype_t *obj);
#endif

#endif
