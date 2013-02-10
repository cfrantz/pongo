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

const dbtype_t DBNULL;

dbtype_t dbboolean_new(pgctx_t *ctx, unsigned val)
{
	dbtype_t obj;
	obj.type = Boolean;
	obj.val = !!val;
	return obj;
}

dbtype_t dbint_new(pgctx_t *ctx, int64_t val)
{
	dbtype_t obj;
	obj.type = Int;
	obj.val = val;
	val >>= 60;
	assert((val == 0 || val == -1) && "val out of range" );
	return obj;
}

dbtype_t dbstrtol(pgctx_t *ctx, dbtype_t s)
{
	dbtype_t ret;
	epstr_t ea;
	dbtag_t type;
	char *ma = NULL;
	char *end = NULL;

	ret.type = Error;
	type = s.type;
	if (type == Int) {
		ret = s;
	} else if (type == ByteBuffer || type == String) {
		ea.all = s.all;
		ea.val[ea.len] = 0;
		ma = (char*)ea.val;
	} else if (!type) {
		s.ptr = dbptr(ctx, s);
		type = s.ptr->type;
		if (type == ByteBuffer || type == String) {
			ma = (char*)s.ptr->sval;
		}
	}
	if (ma) {
		ret.val = strtol(ma, &end, 10);
		if (ma != end && *end == '\0') {
			ret.type = Int;
		} else {
			ret.val = 0;
		}
	}
	return ret;
}


dbtype_t dbfloat_new(pgctx_t *ctx, double val)
{
	dbtype_t obj;
	int64_t *x = (int64_t*)&val;
	obj.type = Float;
	obj.val = *x>>4;
	return obj;
}

dbtype_t _string_new(pgctx_t *ctx, dbtag_t type, const char *val, int len)
{
	uint8_t *s;
	uint32_t hash = 0;
	dbtype_t obj;
	dbstring_t *str;
	uint8_t ch;

	if (len == -1) len = strlen(val);
	if (len < 8) {
		obj.type = type;
		obj.val = len;
		strncpy(((char*)&obj)+1, val, len);
		return obj;
	}
	str = dballoc(ctx, sizeof(dbstring_t) + len +1);
	str->len = len;
	str->type = type;
	s = str->sval;

	hash = 0;
	while(len--) {
		ch = *val++;
		*s++ = ch;
		hash = (hash*31) + ch;
	}
	*s = '\0';
	if (hash == (uint32_t)-1)
		hash = -2;
	str->hash = hash;
	return dboffset(ctx, str);
}

dbtype_t dbbuffer_new(pgctx_t *ctx, const char *val, int len)
{
	return _string_new(ctx, ByteBuffer, val, len);
}

dbtype_t dbstring_new(pgctx_t *ctx, const char *val, int len)
{
	return _string_new(ctx, String, val, len);
}

#ifdef WANT_UUID_TYPE
// FIXME: store uuids in cache?
dbtype_t dbuuid_new(pgctx_t *ctx, uint8_t *val)
{
        dbuuid_t *uu = dballoc(ctx, sizeof(dbuuid_t));
	uu->type = Uuid;
	if (val) {
		memcpy(uu->uuval, val, 16);
	} else {
		uuid_generate_time(uu->uuval);
	}
	return dboffset(ctx, uu);
}

dbtype_t dbuuid_new_fromstring(pgctx_t *ctx, const char *val)
{
        dbuuid_t *uu = dballoc(ctx, sizeof(dbuuid_t));
	uu->type = Uuid;
	if (uuid_parse(val, uu->uuval) < 0) {
		uu = NULL;
	}
	return dboffset(ctx, uu);
}
#endif

dbtype_t dbtime_new(pgctx_t *ctx, int64_t val)
{
	dbtype_t obj;
	obj.type = Datetime;
	obj.val = val;
	return obj;
}

dbtype_t dbtime_newtm(pgctx_t *ctx, struct tm *tm, long usec)
{
	return dbtime_new(ctx, mktimegm(tm) * 1000000LL + usec);
}

dbtype_t dbtime_now(pgctx_t *ctx)
{
	return dbtime_new(ctx, utime_now());
}

// Relational compare (lt, eq, gt)
// NULL is less than all other values
// If types are different, then the "lesser" value is the one with the
// lower type field.  This works out to (Null, Boolean, Int, Datetime, Float,
// Uuid, ByteBuffer, String, List, Object).
// Lists and Objects are compared like strcmp.  Each element is
// examined, key, then value.  If 2 lists/objects are otherwise equal, then
// the shorter one is "less" than the longer one.
//
#define _cmp(a, b) ((a<b) ? -1 : (a>b) ? 1 : 0 )
int dbcmp(pgctx_t *ctx, dbtype_t a, dbtype_t b)
{
	int cmp = 0;
	dbtag_t ta, tb;
	epstr_t ea, eb;
	epfloat_t fa, fb;
	uint8_t *ma, *mb;
	int i, len, alen, blen;
	_list_t *al, *bl;
	_obj_t *ao, *bo;

	if (a.all == b.all) return 0;
	if (!a.all && b.all) return -1;
	if (a.all && !b.all) return 1;
	ma = mb = NULL;
	ta = a.type; tb = b.type;
	if (ta == ByteBuffer || ta == String) {
		ea.all = a.all;
		ea.val[ea.len] = 0;
		ma = ea.val;
	} else if (isPtr(ta)) {
		a.ptr = dbptr(ctx, a);
		ta = a.ptr->type;
		if (ta == ByteBuffer || ta == String) {
			ma = a.ptr->sval;
		}
	}

	if (tb == ByteBuffer || tb == String) {
		eb.all = b.all;
		eb.val[eb.len] = 0;
		mb = eb.val;
	} else if (isPtr(tb)) {
		b.ptr = dbptr(ctx, b);
		tb = b.ptr->type;
		if (tb == ByteBuffer || tb == String) {
			mb = b.ptr->sval;
		}
	}

	if (ta != tb) {
		if ((ta == ByteBuffer && tb == String) ||
		    (ta == String && tb == ByteBuffer)) {
			// Nothing.  Proceed to detailed compare for String and ByteBuffer
		} else {
			return _cmp(ta, tb);
		}
	}

	switch(ta) {
		case Boolean:
		case Int:
		case Datetime:
			cmp = _cmp(a.val, b.val);
			break;
		case Float:
			fa.ival = (int64_t)a.val << 4;
			fb.ival = (int64_t)b.val << 4;
			cmp = _cmp(fa.fval, fb.fval);
			break;
		case Uuid:
			cmp = memcmp(a.ptr->uuval, b.ptr->uuval, 16);
			break;
		case ByteBuffer:
		case String:
			cmp = strcmp((char*)ma, (char*)mb);
			break;
		case List:
			al = dbptr(ctx, a.ptr->list); alen = al->len;
			bl = dbptr(ctx, b.ptr->list); blen = bl->len;
			len = (alen < blen) ? alen : blen;
			for(i=0; i<len; i++) {
				cmp = dbcmp(ctx, al->item[i], bl->item[i]);
				if (cmp) return cmp;
			}
			cmp = _cmp(alen, blen);
			break;
		case Object:
			ao = dbptr(ctx, a.ptr->obj); alen = ao->len;
			bo = dbptr(ctx, b.ptr->obj); blen = bo->len;
			len = (alen < blen) ? alen : blen;
			for(i=0; i<len; i++) {
				cmp = dbcmp(ctx, ao->item[i].key, bo->item[i].key);
				if (cmp) return cmp;
				cmp = dbcmp(ctx, ao->item[i].value, bo->item[i].value);
				if (cmp) return cmp;
			}
			cmp = _cmp(alen, blen);
			break;
		case Collection:
			cmp = -2;
			/*
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
			*/
		default:
			log_error("Comparing unknown object types: %d", ta);
			cmp = -2;
	}
	return cmp;
}

int dbcmp_primitive(pgctx_t *ctx, dbtype_t a, dbtag_t btype, const void *b)
{
	dbtag_t atype;
	epstr_t ea;
	epfloat_t fa;
	uint8_t *ma;
	int cmp;

	if (a.all==0 && b==NULL) return 0;
	if (!a.all && b) return -1;
	if (a.all && !b) return 1;

	ma = NULL;
	atype = a.type;
	if (atype == ByteBuffer || atype == String) {
		ea.all = a.all;
		ea.val[ea.len] = 0;
		ma = ea.val;
	} else if (isPtr(atype)) {
		a.ptr = dbptr(ctx, a);
		atype = a.ptr->type;
		if (atype == ByteBuffer || atype == String) {
			ma = a.ptr->sval;
		}
	}

	if (atype != btype) return _cmp(atype, btype);

	switch(atype) {
		case Boolean:
		case Int:
		case Datetime:
			{ int64_t bval = *(int64_t*)b;
			  cmp = _cmp(a.val, bval);
			}
			break;
		case Float:
			{ double bval = *(double*)b;
			  fa.ival = (int64_t)a.val<<4;
			  cmp = _cmp(fa.fval, bval);
			}
			break;
		case ByteBuffer:
		case String:
			cmp = strcmp((char*)ma, (const char*)b);
			break;
		default:
			log_error("Comparing unknown object types: %d", atype);
			cmp = -2;

	}
	return cmp;
}

char *dbprint(pgctx_t *ctx, dbtype_t t, char *buf, int n)
{
	dbtag_t type;
	epstr_t ea;
	epfloat_t fa;
	int64_t ia;
	char *ma;
	if (!t.all) {
		snprintf(buf, n, "null");
		return buf;
	}
	type = t.type;
	if (type == ByteBuffer || type == String) {
		ea.all = t.all;
		ea.val[ea.len] = 0;
		ma = (char*)ea.val;
	} else if (isPtr(type)) {
		t.ptr = dbptr(ctx, t);
		type = t.ptr->type;
		if (type == ByteBuffer || type == String) {
			ma = (char*)t.ptr->sval;
		}
	}

	switch(type) {
		case Boolean:
			snprintf(buf, n, t.val ? "true" : "false");
			break;
		case Int:
			ia = t.val;
			snprintf(buf, n, "%" PRId64, ia);
			break;
		case Datetime:
			ia = t.val;
			snprintf(buf, n, "datetime(%" PRId64 ")", ia);
			break;
		case Float:
			fa.ival = (int64_t)t.val<<4;
			snprintf(buf, n, "%.3f", fa.fval);
			break;
		case ByteBuffer:
			snprintf(buf, n, "\"%s\"", ma);
			break;
		case String:
			snprintf(buf, n, "u\"%s\"", ma);
			break;
		case Uuid:
			snprintf(buf, n, "uuid(%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x)",
				t.ptr->uuval[0], t.ptr->uuval[1],
				t.ptr->uuval[2], t.ptr->uuval[3],
				t.ptr->uuval[4], t.ptr->uuval[5],
				t.ptr->uuval[6], t.ptr->uuval[7],
				t.ptr->uuval[8], t.ptr->uuval[9],
				t.ptr->uuval[10], t.ptr->uuval[11],
				t.ptr->uuval[12], t.ptr->uuval[13],
				t.ptr->uuval[14], t.ptr->uuval[15]);

			break;
		default:
			snprintf(buf, n, "(type %d)", type);
	}
	return buf;
}
