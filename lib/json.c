#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pongo/json.h>
#include <pongo/log.h>

#include "yajl/yajl_parse.h"



static void stack_put(jsonctx_t *ctx, dbtype_t *value)
{
	dbtype_t *top = ctx->stack[ctx->depth];
	if (ctx->depth == 0) {
		ctx->stack[0] = value;
	} else if (top->type == List) {
		dblist_append(ctx->dbctx, top, value, NOSYNC);
	} else if (top->type == Object) {
        dbtype_t *key = ctx->key[ctx->depth];
        dbobject_setitem(ctx->dbctx, top, key, value, NOSYNC);
	} else {
		log_error("Top of parse stack is neither List nor Object (%d)", top->type);
		abort();
	}
}

static int json_null(void *ctx)
{
	jsonctx_t *c = (jsonctx_t*)ctx;
	//log_verbose("value: null\n");
	stack_put(c, NULL);
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

static int json_string(void *ctx, const unsigned char *val, size_t len)
{
	jsonctx_t *c = (jsonctx_t*)ctx;
	dbtype_t *s = dbstring_new(c->dbctx, (const char*)val, len);
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
	dbtype_t *k = dbstring_new(c->dbctx, (const char*)key, len);
	//log_verbose("key: %s (len=%d)\n", k->sval, len);
	c->key[c->depth] = k;
	return 1;
}

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
#if 0
	if (!strcmp((char*)constructor->sval, "uuid")) {
		dblist_getitem(c->dbctx, args, 0, &value);
		obj = dbuuid_new_fromstring(c->dbctx, (char*)value->sval);
	} else
#endif
    if (!strcmp((char*)constructor->sval, "datetime")) {
		dblist_getitem(c->dbctx, args, 0, &value);
		obj = dbtime_new(c->dbctx, value->ival);
	} else {
		log_error("Unknown __jsonclass__: %s\n", constructor->sval);
		return json;
	}
	dbtypes_free(c->dbctx, json);

	return obj;
}

static int json_map_end(void *ctx)
{
	jsonctx_t *c = (jsonctx_t*)ctx;
	dbtype_t *obj = json_custom_type(ctx, c->stack[c->depth--]);
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
	dbtype_t *list = c->stack[c->depth--];
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

char *json_emit(jsonctx_t *ctx, dbtype_t *db)
{
	yajl_gen g = ctx->json.generator;
    _list_t *list;
    _obj_t *obj;
	int i;
	char buf[40];
    uint8_t *uu;

	if (db == NULL) {
		yajl_gen_null(g);
        return ctx->outstr;
	}

	switch(db->type) {
		case Boolean:
			yajl_gen_bool(g, db->bval); break;
		case Int:
			yajl_gen_integer(g, db->ival); break;
		case Float:
			yajl_gen_double(g, db->fval); break;
		case ByteBuffer:
		case String:
			yajl_gen_string(g, db->sval, db->len); break;
		case Uuid:
			yajl_gen_config(g, yajl_gen_beautify, 0);
			yajl_gen_map_open(g);
			yajl_gen_string(g, "__jsonclass__", 13);
			yajl_gen_array_open(g);
			yajl_gen_string(g, "uuid", 4);
			yajl_gen_array_open(g);
			uu = db->uuval;
			i = sprintf(buf, "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
					uu[0], uu[1], uu[2], uu[3],
					uu[4], uu[5], uu[6], uu[7],
					uu[8], uu[9], uu[10], uu[11],
					uu[12], uu[13], uu[14], uu[15]);
			yajl_gen_string(g, buf, i);
			yajl_gen_array_close(g);
			yajl_gen_array_close(g);
			yajl_gen_map_close(g);
			yajl_gen_config(g, yajl_gen_beautify, 1);
			break;
		case Datetime:
			yajl_gen_config(g, yajl_gen_beautify, 0);
			yajl_gen_map_open(g);
			yajl_gen_string(g, "__jsonclass__", 13);
			yajl_gen_array_open(g);
			yajl_gen_string(g, "datetime", 8);
			yajl_gen_array_open(g);
			yajl_gen_integer(g, db->utctime); 
			yajl_gen_array_close(g);
			yajl_gen_array_close(g);
			yajl_gen_map_close(g);
			yajl_gen_config(g, yajl_gen_beautify, 1);
			break;
		case List:
			yajl_gen_array_open(g);
            list = _ptr(ctx->dbctx, db->list);
			for(i=0; i<list->len; i++)
				json_emit(ctx, _ptr(ctx->dbctx, list->item[i]));
			yajl_gen_array_close(g);
			break;
		case Object:
			yajl_gen_map_open(g);
            obj = _ptr(ctx->dbctx, db->obj);
			for(i=0; i<obj->len; i++) {
                if (obj->item[i].key) {
    				json_emit(ctx, _ptr(ctx->dbctx, obj->item[i].key));
    				json_emit(ctx, _ptr(ctx->dbctx, obj->item[i].value));
                }
			}
			yajl_gen_map_close(g);
			break;
		default:
			log_error("Unknown type: %d at %p\n", db->type, db);

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

dbtype_t *json_parse(jsonctx_t *ctx, char *buf, int len)
{
    if (len == -1)
        len = strlen(buf);
    yajl_parse(ctx->json.parser, buf, len);
    return (ctx->depth == 0) ? ctx->stack[0] : NULL;
}

// vim: ts=4 sts=4 sw=4 expandtab:
