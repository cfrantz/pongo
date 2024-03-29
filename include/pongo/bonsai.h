#ifndef PONGO_BONSAI_H
#define PONGO_BONSAI_H

#include <pongo/dbtypes.h>

extern int bonsai_size(pgctx_t *ctx, dbtype_t node);
extern dbtype_t bonsai_insert(pgctx_t *ctx, dbtype_t node, dbtype_t key, dbtype_t value, int insert_or_fail);
extern dbtype_t bonsai_multi_insert(pgctx_t *ctx, dbtype_t node, dbtype_t key, dbtype_t value);

extern dbtype_t bonsai_delete(pgctx_t *ctx, dbtype_t node, dbtype_t key, dbtype_t *valout);
extern dbtype_t bonsai_multi_delete(pgctx_t *ctx, dbtype_t node, dbtype_t key, dbtype_t value);

extern int bonsai_find(pgctx_t *ctx, dbtype_t node, dbtype_t key, dbtype_t *value);
extern dbtype_t bonsai_find_node(pgctx_t *ctx, dbtype_t node, dbtype_t key);
extern dbtype_t bonsai_find_primitive(pgctx_t *ctx, dbtype_t node, dbtag_t type, const void *key);
//extern dbtype_t bonsai_index(pgctx_t *ctx, dbtype_t node, int index);

typedef void (*bonsaicb_t)(pgctx_t *ctx, dbtype_t node, void *user);
extern void bonsai_foreach(pgctx_t *ctx, dbtype_t node, bonsaicb_t cb, void *user);
extern void bonsai_show(pgctx_t *ctx, dbtype_t node, int depth);

#endif
