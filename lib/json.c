#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pongo/bonsai.h>
#include <pongo/json.h>
#include <pongo/log.h>

#include "yajl/yajl_parse.h"

#define UC(x) ((unsigned char*)(x))
#define UUID_FMT(uu, n) "uuid(%02hhx%02hhx%02hhx%02hhx-%02hhx%02hhx-%02hhx%02hhx-%02hhx%02hhx-%02hhx%02hhx%02hhx%02hhx%02hhx%02hhx)%n", \
	uu+0, uu+1, uu+2, uu+3, \
	uu+4, uu+5, uu+6, uu+7, \
	uu+8, uu+9, uu+10, uu+11, \
	uu+12, uu+13, uu+14, uu+15, n


static void stack_put(jsonctx_t *ctx, dbtype_t value)
{
	dbtype_t top = ctx->stack[ctx->depth];
    dbtag_t type = dbtype(ctx->dbctx, top);

	if (ctx->depth == 0) {
		ctx->stack[0] = value;
	} else if (type == List) {
		dblist_append(ctx->dbctx, top, value, NOSYNC);
	} else if (type == Object) {
        dbtype_t key = ctx->key[ctx->depth];
        dbobject_setitem(ctx->dbctx, top, key, value, NOSYNC);
	} else {
		log_error("Top of parse stack is neither List nor Object (%d)", type);
		abort();
	}
}

static int json_null(void *ctx)
{
	jsonctx_t *c = (jsonctx_t*)ctx;
	//log_verbose("value: null\n");
	stack_put(c, DBNULL);
	return 1;
}

static int json_boolean(void *ctx, int val)
{
	jsonctx_t *c = (jsonctx_t*)ctx;
	//log_verbose("value: boolean(%lld)\n", val);
	stack_put(c, dbboolean_new(c->dbctx, val));
	return 1;
}

static int json_integer(void *ctx, long long val)
{
	jsonctx_t *c = (jsonctx_t*)ctx;
	//log_verbose("value: int(%lld)\n", val);
	stack_put(c, dbint_new(c->dbctx, val));
	return 1;
}

static int json_double(void *ctx, double val)
{
	jsonctx_t *c = (jsonctx_t*)ctx;
	//log_verbose("value: double(%d)\n", val);
	stack_put(c, dbfloat_new(c->dbctx, val));
	return 1;
}

static int json_string(void *ctx, const unsigned char *value, size_t len)
{
	jsonctx_t *c = (jsonctx_t*)ctx;
    const char *val = (const char *)value;
    dbtype_t s;
    uint8_t buf[16];
    int64_t dt;
    int n = 0;

    if (sscanf(val, UUID_FMT(buf, &n)) == 17 && n==len) {
        s = dbuuid_new(c->dbctx, buf);
    } else if (sscanf(val, "datetime(%" PRId64 ")%n", &dt, &n) == 2 && n==len) {
		s = dbtime_new(c->dbctx, dt);
    } else {
	    s = dbstring_new(c->dbctx, val, len);
    }
   	//log_verbose("value: string(%d, %s) at %p\n", len, s->sval, s);
	stack_put(c, s);
	return 1;
}

static int json_map_start(void *ctx)
{
	jsonctx_t *c = (jsonctx_t*)ctx;
	//log_verbose("map: {\n");
	c->stack[++c->depth] = dbobject_new(c->dbctx);
	return 1;
}

static int json_map_key(void *ctx, const unsigned char *key, size_t len)
{
	jsonctx_t *c = (jsonctx_t*)ctx;
	dbtype_t k = dbstring_new(c->dbctx, (const char*)key, len);
	//log_verbose("key: %s (len=%d)\n", k->sval, len);
	c->key[c->depth] = k;
	return 1;
}

#if 0
static dbtype_t *json_custom_type(void *ctx, dbtype_t *json)
{
	jsonctx_t *c = (jsonctx_t*)ctx;
	dbtype_t *obj = NULL;
	dbtype_t *list;
	dbtype_t *constructor;
	dbtype_t *value;
	dbtype_t *args;

	if (dbobject_getstr(c->dbctx, json, "__jsonclass__", &list) == -1)
		return json;
	dblist_getitem(c->dbctx, list, 0, &constructor);
	dblist_getitem(c->dbctx, list, 1, &args);

	if (!strcmp((char*)constructor->sval, "uuid")) {
		dblist_getitem(c->dbctx, args, 0, &value);
		obj = dbuuid_new_fromstring(c->dbctx, (char*)value->sval);
	} else if (!strcmp((char*)constructor->sval, "datetime")) {
		dblist_getitem(c->dbctx, args, 0, &value);
		obj = dbtime_new(c->dbctx, value->ival);
	} else {
		log_error("Unknown __jsonclass__: %s\n", constructor->sval);
		return json;
	}
	return obj;
}
#endif

static int json_map_end(void *ctx)
{
	jsonctx_t *c = (jsonctx_t*)ctx;
	dbtype_t obj = c->stack[c->depth--];
	//obj = json_custom_type(ctx, obj);
	//log_verbose("map: }\n");
	stack_put(c, obj);
	return 1;
}

static int json_array_start(void *ctx)
{
	jsonctx_t *c = (jsonctx_t*)ctx;
	//log_verbose("array: [\n");
	c->stack[++c->depth] = dblist_new(c->dbctx);
	return 1;
}

static int json_array_end(void *ctx)
{
	jsonctx_t *c = (jsonctx_t*)ctx;
	dbtype_t list = c->stack[c->depth--];
	//log_verbose("array: ]\n");
	stack_put(c, list);
	return 1;
}

static yajl_callbacks json_callbacks = {
	json_null,
	json_boolean,
	json_integer,
	json_double,
	NULL,
	json_string,
	json_map_start,
	json_map_key,
	json_map_end,
	json_array_start,
	json_array_end
};

static void json_print(void *ctx, const char *str, size_t len)
{
	jsonctx_t *c = (jsonctx_t*)ctx;
    unsigned chunk;
    if (c->outfp) {
        fwrite(str, 1, len, c->outfp);
    } else {
        if (c->outstr == NULL)
            c->outlen = 0;
        chunk = (c->outlen+65535) & ~65535;
        if (c->outlen + len > chunk) {
            chunk = (c->outlen + len + 65535) & ~65535;
            c->outstr = realloc(c->outstr, chunk);
        }
        memcpy(c->outstr+c->outlen, str, len);
        c->outlen += len;
        c->outstr[c->outlen] = 0;
    }
}

static void collection_helper(pgctx_t *ctx, dbtype_t node, void *user)
{
    jsonctx_t *j = (jsonctx_t*)user;
    node.ptr = dbptr(ctx, node);
    json_emit(j, node.ptr->key);
    json_emit(j, node.ptr->value);
}

char *json_emit(jsonctx_t *ctx, dbtype_t db)
{
	yajl_gen g = ctx->json.generator;
    dbtag_t type;
    epstr_t ea;
    epfloat_t fa;
    uint8_t *ma = NULL;
    uint32_t len = 0;
    _list_t *list;
    _obj_t *obj;
	int i;
	char buf[64];
    uint8_t *uu;

	if (db.all == 0) {
		yajl_gen_null(g);
        return ctx->outstr;
	}

    type = db.type;
    if (type == String || type == ByteBuffer) {
        ea.all = db.all;
        len = ea.len;
        ea.val[len] = 0;
        ma = ea.val;
    } else if (isPtr(type)) {
        db.ptr = dbptr(ctx->dbctx, db);
        type = db.ptr->type;
        if (type == String || type == ByteBuffer) {
            len = db.ptr->len;
            ma = db.ptr->sval;
        }
    }

	switch(type) {
		case Boolean:
			yajl_gen_bool(g, db.val); break;
		case Int:
			yajl_gen_integer(g, db.val); break;
		case Float:
            fa.ival = (int64_t)db.val << 4;
			yajl_gen_double(g, fa.fval); break;
		case ByteBuffer:
		case String:
			yajl_gen_string(g, ma, len); break;
		case Uuid:
#if 0
			yajl_gen_config(g, yajl_gen_beautify, 0);
			yajl_gen_map_open(g);
			yajl_gen_string(g, UC("__jsonclass__"), 13);
			yajl_gen_array_open(g);
			yajl_gen_string(g, UC("uuid"), 4);
			yajl_gen_array_open(g);
			uu = db->uuval;
			i = sprintf(buf, "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
					uu[0], uu[1], uu[2], uu[3],
					uu[4], uu[5], uu[6], uu[7],
					uu[8], uu[9], uu[10], uu[11],
					uu[12], uu[13], uu[14], uu[15]);
			yajl_gen_string(g, UC(buf), i);
			yajl_gen_array_close(g);
			yajl_gen_array_close(g);
			yajl_gen_map_close(g);
			yajl_gen_config(g, yajl_gen_beautify, 1);
#else
			uu = db.ptr->uuval;
			i = sprintf(buf, "uuid(%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x)",
					uu[0], uu[1], uu[2], uu[3],
					uu[4], uu[5], uu[6], uu[7],
					uu[8], uu[9], uu[10], uu[11],
					uu[12], uu[13], uu[14], uu[15]);
			yajl_gen_string(g, UC(buf), i); break;
#endif
			break;
		case Datetime:
#if 0
			yajl_gen_config(g, yajl_gen_beautify, 0);
			yajl_gen_map_open(g);
			yajl_gen_string(g, UC("__jsonclass__"), 13);
			yajl_gen_array_open(g);
			yajl_gen_string(g, UC("datetime"), 8);
			yajl_gen_array_open(g);
			yajl_gen_integer(g, db->utctime); 
			yajl_gen_array_close(g);
			yajl_gen_array_close(g);
			yajl_gen_map_close(g);
			yajl_gen_config(g, yajl_gen_beautify, 1);
#else
                        i = sprintf(buf, "datetime(%" PRId64 ")", (int64_t)db.val);
			yajl_gen_string(g, UC(buf), i); break;
#endif
			break;
		case List:
			yajl_gen_array_open(g);
            list = dbptr(ctx->dbctx, db.ptr->list);
			for(i=0; i<list->len; i++)
				json_emit(ctx, list->item[i]);
			yajl_gen_array_close(g);
			break;
		case Object:
			yajl_gen_map_open(g);
            obj = dbptr(ctx->dbctx, db.ptr->obj);
			for(i=0; i<obj->len; i++) {
				json_emit(ctx, obj->item[i].key);
   				json_emit(ctx, obj->item[i].value);
			}
			yajl_gen_map_close(g);
			break;
		case Collection:
		case Cache:
			yajl_gen_map_open(g);
            bonsai_foreach(ctx->dbctx, db.ptr->obj, collection_helper, ctx);
			yajl_gen_map_close(g);
            break;
		default:
			log_error("Unknown type: %d at %" PRIx64 "\n", type, db.all);

	}
    return ctx->outstr;
}


jsonctx_t *json_init(pgctx_t *dbctx)
{

    jsonctx_t *ctx = malloc(sizeof(jsonctx_t));
    memset(ctx, 0, sizeof(jsonctx_t));
	ctx->dbctx = dbctx;
	ctx->depth = 0;
	ctx->json.parser = yajl_alloc(&json_callbacks, NULL, ctx);
	//yajl_config(ctx->json.parser, yajl_allow_multiple_values, 1);
	//yajl_config(ctx->json.parser, yajl_allow_comments, 1);

	ctx->json.generator = yajl_gen_alloc(NULL); 
	yajl_gen_config(ctx->json.generator, yajl_gen_beautify, 1);
	yajl_gen_config(ctx->json.generator, yajl_gen_print_callback, json_print, ctx);
    return ctx;
}

void json_cleanup(jsonctx_t *ctx)
{
    yajl_free(ctx->json.parser);
    yajl_gen_free(ctx->json.generator);
    free(ctx->outstr);
    free(ctx);
}

dbtype_t json_parse(jsonctx_t *ctx, char *buf, int len)
{
    if (len == -1)
        len = strlen(buf);
    yajl_parse(ctx->json.parser, UC(buf), len);
    return (ctx->depth == 0) ? ctx->stack[0] : DBNULL;
}

void json_dump(pgctx_t *dbctx, dbtype_t obj, FILE *fp)
{
    jsonctx_t *ctx = json_init(dbctx);
    ctx->outfp = fp;
    json_emit(ctx, obj);
    json_cleanup(ctx);
}

// vim: ts=4 sts=4 sw=4 expandtab:
