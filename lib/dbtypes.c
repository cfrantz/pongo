#include <stdio.h>
#include <string.h>
#ifdef WANT_UUID_TYPE
#include <uuid/uuid.h>
#endif
#include <pongo/dbmem.h>
#include <pongo/dbtypes.h>
#include <pongo/bonsai.h>
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
		obj = dbcache_get_str(ctx, type, val, len);
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
	if (hash == (uint32_t)-1)
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
// FIXME: store uuids in cache?
dbtype_t *dbuuid_new(pgctx_t *ctx, uint8_t *val)
{
	dbtype_t *obj = dballoc(ctx, sizeof(dbuuid_t));
	obj->type = Uuid;
	if (val) {
		memcpy(obj->uuval, val, 16);
	} else {
		uuid_generate_time(obj->uuval);
	}
	return obj;
}

dbtype_t *dbuuid_new_fromstring(pgctx_t *ctx, const char *val)
{
	dbtype_t *obj = dballoc(ctx, sizeof(dbuuid_t));
	obj->type = Uuid;
	if (uuid_parse(val, obj->uuval) < 0) {
		obj = NULL;
	}
	return obj;
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
// NULL is less than all other values
// If types are different, then the "lesser" value is the one with the
// lower type field.  This works out to (Null, Boolean, Int, Datetime, Float,
// Uuid, ByteBuffer, String, List, Object).
// Lists and Objects are compared like strcmp.  Each element is
// examined, key, then value.  If 2 lists/objects are otherwise equal, then
// the shorter one is "less" than the longer one.
int dbcmp(pgctx_t *ctx, dbtype_t *a, dbtype_t *b)
{
	int cmp = 0;
	int i, len, alen, blen;
	_list_t *al, *bl;
	_obj_t *ao, *bo;
	dbtype_t *aa, *bb;
	double diff;
	int64_t diff64;

	if (a == b) return 0;
	if (!a && b) return -1;
	if (a && !b) return 1;
	if (a->type != b->type) {
		if ((a->type == ByteBuffer && b->type == String) ||
		    (a->type == String && b->type == ByteBuffer)) {
			// Nothing
		} else {
			return a->type - b->type;
		}

		/*
		 * Not sure if I want mixed type compare among numbers
		 *
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
		*/
	}
	switch(a->type) {
		case Boolean:
		case Int:
		case Datetime:
			diff64 = a->ival - b->ival;
			if (diff64 < 0) { cmp = -1; }
			else if (diff64 > 0) { cmp = 1; }
			else { cmp = 0; }
			break;
		case Float:
			diff = a->fval - b->fval;
			if (diff < 0.0) { cmp = -1; }
			else if (diff > 0.0) { cmp = 1; }
			else { cmp = 0; }
			break;
		case Uuid:
			cmp = memcmp(a->uuval, b->uuval, 16);
			break;
		case ByteBuffer:
		case String:
			cmp = strcmp((char*)a->sval, (char*)b->sval);
			break;
		case List:
			al = _ptr(ctx, a->list); alen = al->len;
			bl = _ptr(ctx, b->list); blen = bl->len;
			len = (alen < blen) ? alen : blen;
			for(i=0; i<len; i++) {
				cmp = dbcmp(ctx, _ptr(ctx, al->item[i]), _ptr(ctx, bl->item[i]));
				if (cmp) return cmp;
			}
			cmp = alen-blen;
			break;
		case Object:
			ao = _ptr(ctx, a->obj); alen = ao->len;
			bo = _ptr(ctx, b->obj); blen = bo->len;
			len = (alen < blen) ? alen : blen;
			for(i=0; i<len; i++) {
				cmp = dbcmp(ctx, _ptr(ctx, ao->item[i].key), _ptr(ctx, bo->item[i].key));
				if (cmp) return cmp;
				cmp = dbcmp(ctx, _ptr(ctx, ao->item[i].value), _ptr(ctx, bo->item[i].value));
				if (cmp) return cmp;
			}
			cmp = alen-blen;
			break;
		case Collection:
			a = _ptr(ctx, a->obj); alen = bonsai_size(a);
			b = _ptr(ctx, b->obj); blen = bonsai_size(b);
			len = (alen < blen) ? alen : blen;
			for(i=0; i<len; i++) {
				aa = bonsai_index(ctx, a, i);
				bb = bonsai_index(ctx, b, i);
				cmp = dbcmp(ctx, _ptr(ctx, aa->key), _ptr(ctx, bb->key));
				if (cmp) return cmp;
				cmp = dbcmp(ctx, _ptr(ctx, aa->value), _ptr(ctx, bb->value));
				if (cmp) return cmp;
			}
			cmp = alen - blen;
			break;
		default:
			log_error("Comparing unknown object types: %d", a->type);
			cmp = -2;
	}
	return cmp;
}

int dbcmp_primitive(dbtype_t *a, dbtag_t btype, const void *b)
{
	int cmp;
	double diff;
	int64_t diff64;

	if (a==NULL && b==NULL) return 0;
	if (!a && b) return -1;
	if (a && !b) return 1;
	if (a->type != btype) return a->type - btype;

	switch(a->type) {
		case Boolean:
		case Int:
		case Datetime:
			diff64 = a->ival - *(int64_t*)b;
			if (diff64 < 0) { cmp = -1; }
			else if (diff64 > 0) { cmp = 1; }
			else { cmp = 0; }
			break;
		case Float:
			diff = a->fval - *(double*)b;
			if (diff < 0.0) { cmp = -1; }
			else if (diff > 0.0) { cmp = 1; }
			else { cmp = 0; }
			break;
		case ByteBuffer:
		case String:
			cmp = strcmp((char*)a->sval, (const char*)b);
			break;
		default:
			log_error("Comparing unknown object types: %d", a->type);
			cmp = -2;

	}
	return cmp;
}


char *dbprint(dbtype_t *t, char *buf, int n)
{
	if (t==NULL) {
		snprintf(buf, n, "null");
		return buf;
	}
	switch(t->type) {
		case Boolean:
			snprintf(buf, n, t->bval ? "true" : "false");
			break;
		case Int:
			snprintf(buf, n, "%" PRId64, t->ival);
			break;
		case Datetime:
			snprintf(buf, n, "datetime(%" PRId64 ")", t->ival);
			break;
		case Float:
			snprintf(buf, n, "%.3f", t->fval);
			break;
		case ByteBuffer:
			snprintf(buf, n, "\"%s\"", t->sval);
			break;
		case String:
			snprintf(buf, n, "u\"%s\"", t->sval);
			break;
		default:
			snprintf(buf, n, "(type %d)", t->type);
	}
	return buf;
}
