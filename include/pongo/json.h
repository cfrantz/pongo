#ifndef PONGO_JSON_H
#define PONGO_JSON_H

#include <pongo/dbtypes.h>
#include <pongo/context.h>
#include <yajl/yajl_parse.h>
#include <yajl/yajl_gen.h>

typedef struct {
	pgctx_t *dbctx;
	dbtype_t *stack[64];
	dbtype_t *key[64];
	int depth;
	struct {
		yajl_handle parser;
		yajl_gen generator;
	} json;
    char *outstr;
    unsigned outlen;
} jsonctx_t;

extern jsonctx_t *json_init(pgctx_t *dbctx);
extern void json_cleanup(jsonctx_t *ctx);
extern dbtype_t *json_parse(jsonctx_t *ctx, char *buf, int len);
extern char *json_emit(jsonctx_t *ctx, dbtype_t *obj);
#endif
