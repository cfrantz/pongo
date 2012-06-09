#include <string.h>
#ifdef WANT_UUID_TYPE
#include <uuid/uuid.h>
#endif
#include <pongo/dbmem.h>
#include <pongo/dbtypes.h>
#include <pongo/log.h>
#include <pongo/misc.h>

#if 1
#define cache_put(ctx, obj) dbcache_put(ctx, obj)
#else
#define cache_put(ctx, obj) (obj)
#endif

dbtype_t *dbboolean_new(pgctx_t *ctx, unsigned val)
{
	return _ptr(ctx, ctx->root->booleans[!!val]);
}

dbtype_t *dbint_new(pgctx_t *ctx, int64_t val)
{
	dbtype_t *obj;
	//if (val >= -5 && val < 256) {
	//	return _ptr(ctx, ctx->root->integers+(5+val)*16);
	//}
	obj = dbcache_get_int(ctx, Int, val);
	if (obj) return obj;

	obj = dballoc(ctx, sizeof(dbint_t));
	obj->type = Int;
	obj->ival = val;
	return cache_put(ctx, obj);
}


dbtype_t *dbfloat_new(pgctx_t *ctx, double val)
{
	dbtype_t *obj;
	obj = dbcache_get_float(ctx, Float, val);
	if (obj) return obj;

	obj = dballoc(ctx, sizeof(dbfloat_t));
	obj->type = Float;
	obj->fval = val;
	return cache_put(ctx, obj);
}

dbtype_t *_string_new(pgctx_t *ctx, dbtag_t type, const char *val, int len)
{
	uint8_t *s;
	uint32_t hash = 0;
	dbtype_t *obj;
	uint8_t ch;

	if (len == -1) len = strlen(val);
	if (len < 128) {
		obj = dbcache_get_str(ctx, type, val);
		if (obj) return obj;
	}
	obj = dballoc(ctx, sizeof(dbstring_t) + len +1);
	obj->len = len;
	obj->type = type;
	s = obj->sval;

	hash = 0;
	while(len--) {
		ch = *val++;
		*s++ = ch;
		hash = (hash*31) + ch;
	}
	*s = '\0';
	if (hash == -1UL)
		hash = -2;
	obj->hash = hash;
	return cache_put(ctx, obj);
}

dbtype_t *dbbuffer_new(pgctx_t *ctx, const char *val, int len)
{
	return _string_new(ctx, ByteBuffer, val, len);
}

dbtype_t *dbstring_new(pgctx_t *ctx, const char *val, int len)
{
	return _string_new(ctx, String, val, len);
}

#ifdef WANT_UUID_TYPE
dbtype_t *dbuuid_new(pgctx_t *ctx, uint8_t *val)
{
	dbtype_t *obj = dballoc(ctx, sizeof(dbuuid_t));
	obj->type = Uuid;
	if (val) {
		memcpy(obj->uuval, val, 16);
	} else {
		uuid_generate_time(obj->uuval);
	}
	return cache_put(ctx, obj);
}

dbtype_t *dbuuid_new_fromstring(pgctx_t *ctx, const char *val)
{
	dbtype_t *obj = dballoc(ctx, sizeof(dbuuid_t));
	obj->type = Uuid;
	if (uuid_parse(val, obj->uuval) < 0) {
		dbfree(ctx, obj);
		obj = NULL;
	}
	return cache_put(ctx, obj);
}
#endif

dbtype_t *dbtime_new(pgctx_t *ctx, int64_t val)
{
	dbtype_t *obj;
	obj = dbcache_get_int(ctx, Datetime, val);
	if (obj) return obj;

	obj = dballoc(ctx, sizeof(dbtime_t));
	obj->type = Datetime;
	obj->utctime = val;
	return cache_put(ctx, obj);
}

dbtype_t *dbtime_newtm(pgctx_t *ctx, struct tm *tm, long usec)
{
	return dbtime_new(ctx, mktimegm(tm) * 1000000LL + usec);
}

dbtype_t *dbtime_now(pgctx_t *ctx)
{
	return dbtime_new(ctx, utime_now());
}

uint32_t dbhashval(dbtype_t *a)
{
	int32_t hash;
	int32_t *p;

	if (a == 0 || (long)a == -1L) return 0;
	switch(a->type) {
		case Boolean:
		case Int:
		case Float:
		case Datetime:
			hash = (int32_t)a->ival ^ (a->ival>>32);
			break;
		case String:
		case ByteBuffer:
			hash = a->hash;
			break;
		case Uuid:
			p = (int32_t*)a->uuval;
			hash = p[0] ^ p[1] ^ p[2] ^ p[3];
			break;
		case List:
		case Object:
		case _InternalList:
		case _InternalObj:
			// List and Object don't have hashvals
			return -1;
		default:
			log_error("dbhashval for unknown object type %d at %p", a->type, a);
			return -1;

	}
	if (hash == -1) hash = -2;
	return hash;
}

// Relational compare (lt, eq, gt)
int dbcmp(dbtype_t *a, dbtype_t *b)
{
	int cmp = 0;
	double diff;

	if (a == b) return 0;
	if (!(a && b)) return -2;
	if (a->type != b->type) {
		if ((a->type == Int && b->type == Float)) {
			diff = a->ival - b->fval;
			if (diff < 0.0) { cmp = -1; }
			else if (diff > 0.0) { cmp = 1; }
			return cmp;
		} else if ((a->type == Float && b->type == Int)) {
			diff = a->fval - b->ival;
			if (diff < 0.0) { cmp = -1; }
			else if (diff > 0.0) { cmp = 1; }
			return cmp;
		}
		return -2;
	}
	switch(a->type) {
		case Boolean:
		case Int:
		case Datetime:
			cmp = a->ival - b->ival;
			if (cmp < 0) { cmp = -1; }
			else if (cmp > 0) { cmp = 1; }
			break;
		case Float:
			diff = a->fval - b->fval;
			if (diff < 0.0) { cmp = -1; }
			else if (diff > 0.0) { cmp = 1; }
			break;
		case ByteBuffer:
		case String:
			cmp = strcmp((char*)a->sval, (char*)b->sval);
			break;
		case Uuid:
			cmp = memcmp(a->uuval, b->uuval, 16);
			break;
		case List:
		case Object:
		case _InternalList:
		case _InternalObj:
			// Currently "cmp" has no meaning for
			// containers
			cmp = -2;
			break;
		default:
			log_error("Comparing unknown object types: %d", a->type);
			cmp = -2;
	}
	return cmp;
}

// Simple value equality
int dbeq(dbtype_t *a, dbtype_t *b, int mixed)
{
	int cmp = 0;

	// If they're both NULL they're equal.
	if (a == b) return 1;
	// If only one is NULL, they're not equal
	if (a==NULL || b==NULL) return 0;

	if ((a->type == ByteBuffer && b->type == String) ||
	    (b->type == ByteBuffer && a->type == String)) {
		// do nothing and fall into the switch below
	} else if (mixed && a->type != b->type) {
		if ((a->type == Int && b->type == Float)) {
			return a->ival == b->fval;
		} else if ((a->type == Float && b->type == Int)) {
			return a->fval == b->ival;
		}
		return 0;
	}
	switch(a->type) {
		case Boolean:
		case Int:
		case Datetime:
			cmp = a->ival == b->ival;
			break;
		case Float:
			cmp = a->fval == b->fval;
			break;
		case ByteBuffer:
		case String:
			if (a->hash == b->hash)
				cmp = !strcmp((char*)a->sval, (char*)b->sval);
			break;
		case Uuid:
			cmp = !memcmp(a->uuval, b->uuval, 16);
			break;
		case List:
		case Object:
		case _InternalList:
		case _InternalObj:
			// Currently "eq" has no meaning for
			// containers
			cmp = 0;
			break;
		default:
			log_error("Comparing unknown object types: %d", a->type);
			cmp = 0;
	}
	return cmp;
}
