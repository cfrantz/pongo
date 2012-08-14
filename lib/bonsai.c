#include <stdio.h>
#include <limits.h>
#include <pongo/dbmem.h>
#include <pongo/context.h>
#include <pongo/dbtypes.h>
#include <pongo/bonsai.h>


// A little shortcut for all the silly ctx pointer/offset conversions
#define GET(x) ((dbtype_t*)_ptr(ctx, x))
#define WEIGHT 2

typedef enum {
    subtree_left, subtree_right
} subtree_t;

int
bonsai_size(dbtype_t *node)
{
	return node ? node->size: 0;
}

static dbtype_t *
bonsai_new(pgctx_t *ctx, dbtype_t *left, dbtype_t *right, dbtype_t *key, dbtype_t *value)
{
    dbtype_t *node;

    node = dballoc(ctx, sizeof(dbnode_t));
    node->type = _BonsaiNode;
    node->left = _offset(ctx, left);
    node->right = _offset(ctx, right);
    node->size = 1 + bonsai_size(left) + bonsai_size(right);
    node->key = _offset(ctx, key);
    node->value = _offset(ctx, value);
    return node;
}

static dbtype_t *
single_left(pgctx_t *ctx, dbtype_t *left, dbtype_t *right, dbtype_t *key, dbtype_t *value)
{
    dbtype_t *ret;
    ret = bonsai_new(ctx,
            bonsai_new(ctx, left, GET(right->left), key, value),
            GET(right->right),
            GET(right->key), GET(right->value));
    return ret;
}

static dbtype_t *
double_left(pgctx_t *ctx, dbtype_t *left, dbtype_t *right, dbtype_t *key, dbtype_t *value)
{
    dbtype_t *ret;
    ret = bonsai_new(ctx,
            bonsai_new(ctx, left, GET(GET(right->left)->left), key, value),
            bonsai_new(ctx, GET(GET(right->left)->right), GET(right->right), GET(right->key), GET(right->value)),
                    GET(GET(right->left)->key), GET(GET(right->left)->value));
    return ret;
}

static dbtype_t *
single_right(pgctx_t *ctx, dbtype_t *left, dbtype_t *right, dbtype_t *key, dbtype_t *value)
{
    dbtype_t *ret;
    ret = bonsai_new(ctx,
            GET(left->left),
            bonsai_new(ctx, GET(left->right), right, key, value),
            GET(left->key), GET(left->value));
    return ret;
}

static dbtype_t *
double_right(pgctx_t *ctx, dbtype_t *left, dbtype_t *right, dbtype_t *key, dbtype_t *value)
{
    dbtype_t *ret;
    ret = bonsai_new(ctx,
            bonsai_new(ctx, GET(left->left), GET(GET(left->right)->left), GET(left->key), GET(left->value)),
            bonsai_new(ctx, GET(GET(left->right)->right), right, key, value),
            GET(GET(left->right)->key), GET(GET(left->right)->value));
    return ret;
}

static dbtype_t *
balance_left(pgctx_t *ctx, dbtype_t *left, dbtype_t *right, dbtype_t *key, dbtype_t *value)
{
    uint64_t rln = bonsai_size(GET(right->left));
    uint64_t rrn = bonsai_size(GET(right->right));
    return (rln < rrn) ?
        single_left(ctx, left, right, key, value) :
        double_left(ctx, left, right, key, value) ;
}

static dbtype_t *
balance_right(pgctx_t *ctx, dbtype_t *left, dbtype_t *right, dbtype_t *key, dbtype_t *value)
{
    uint64_t lln = bonsai_size(GET(left->left));
    uint64_t lrn = bonsai_size(GET(left->right));
    return (lrn < lln) ?
        single_right(ctx, left, right, key, value) :
        double_right(ctx, left, right, key, value) ;
}

static inline dbtype_t *
balance(pgctx_t *ctx, dbtype_t *cur, dbtype_t *left, dbtype_t *right, subtree_t which, int inplace)
{
    uint64_t ln = bonsai_size(left);
    uint64_t rn = bonsai_size(right);
    dbtype_t *key = GET(cur->key);
    dbtype_t *value = GET(cur->value);
    dbtype_t *ret;

    if (ln+rn < 2)
        goto balanced;
    if (rn > WEIGHT * ln)
        ret = balance_left(ctx, left, right, key, value);
    else if (ln > WEIGHT * rn)
        ret = balance_right(ctx, left, right, key, value);
    else
        goto balanced;

    return ret;
balanced:
    inplace = 0;
    if (inplace) {
        if (which == subtree_left) {
            // rcu_assign_pointer
            cur->left = _offset(ctx, left);
        } else {
            // rcu_assign_pointer
            cur->right = _offset(ctx, right);
        }
        cur->size = 1 + bonsai_size(left) + bonsai_size(right);
        ret = cur;
    } else {
        ret = bonsai_new(ctx, left, right, key, value);
    }
    return ret;
}

dbtype_t *
bonsai_insert(pgctx_t *ctx, dbtype_t *node, dbtype_t *key, dbtype_t *value, int insert_or_fail)
{
    int cmp;
    if (!node)
        return bonsai_new(ctx, NULL, NULL, key, value);

    cmp = dbcmp(ctx, key, GET(node->key));
    if (insert_or_fail && cmp==0)
        return BONSAI_ERROR;
    if (cmp < 0) {
        return balance(ctx, node,
                bonsai_insert(ctx, GET(node->left), key, value, insert_or_fail),
                GET(node->right),
                subtree_left, 1);
    }
    if (cmp > 0) {
        return balance(ctx, node,
                GET(node->left),
                bonsai_insert(ctx, GET(node->right), key, value, insert_or_fail),
                subtree_right, 1);
    }
    node->value = _offset(ctx, value);
    return node;
}

static dbtype_t *
_bonsai_insert_index(pgctx_t *ctx, dbtype_t *node, int index, dbtype_t *value)
{
    int sz;
    if (!node)
        return bonsai_new(ctx, NULL, NULL, NULL, value);

    sz = bonsai_size(GET(node->left));
    if (index < sz) {
        return balance(ctx, node,
                _bonsai_insert_index(ctx, GET(node->left), index, value),
                GET(node->right),
                subtree_left, 1);
    }
    if (index > sz) {
        return balance(ctx, node,
                GET(node->left),
                _bonsai_insert_index(ctx, GET(node->right), index-(sz+1), value),
                subtree_right, 1);
    }
    return balance(ctx, 
            bonsai_new(ctx, NULL, NULL, NULL, value),
            GET(node->left),
            node,
            subtree_right, 1);
}

dbtype_t *
bonsai_insert_index(pgctx_t *ctx, dbtype_t *node, int index, dbtype_t *value)
{
    int sz = bonsai_size(node);
    if (index < 0)
        index += sz;

    if (index == INT_MAX)
        index = sz;
    else if (index && (index < 0 || index >= sz))
        return BONSAI_ERROR;

    return _bonsai_insert_index(ctx, node, index, value);
}

static dbtype_t *
delete_min(pgctx_t *ctx, dbtype_t *node, dbtype_t **out)
{
    dbtype_t *left = GET(node->left), *right = GET(node->right);

    if (!left) {
        *out = node;
        return right;
    }
    return balance(ctx, node, delete_min(ctx, left, out), right, subtree_left, 0);
}

static dbtype_t *
_bonsai_delete(pgctx_t *ctx, dbtype_t *node, dbtype_t *key, dbtype_t **valout)
{
    dbtype_t *min, *left, *right;
    int cmp;

    if (!node) {
        if (valout) *valout = BONSAI_ERROR;
        return NULL;
    }

    left = GET(node->left);
    right = GET(node->right);
    cmp = dbcmp(ctx, key, GET(node->key));
    if (cmp < 0) {
        return balance(ctx, node, _bonsai_delete(ctx, left, key, valout), right, subtree_left, 1);
    }
    if (cmp > 0) {
        return balance(ctx, node, left, _bonsai_delete(ctx, right, key, valout), subtree_right, 1);
    }

    if (valout) *valout = GET(node->value);
    if (!left) return right;
    if (!right) return left;
    right = delete_min(ctx, right, &min);
    return balance(ctx, min, left, right, subtree_right, 0);
}

dbtype_t *
bonsai_delete(pgctx_t *ctx, dbtype_t *node, dbtype_t *key, dbtype_t **value)
{
    dbtype_t *valout = NULL;
    dbtype_t *ret = _bonsai_delete(ctx, node, key, &valout);
    if (value) *value = valout;
    return (valout == BONSAI_ERROR) ? valout : ret;
}

static dbtype_t *
_bonsai_delete_index(pgctx_t *ctx, dbtype_t *node, int index, dbtype_t **valout)
{
    dbtype_t *min, *left, *right;
    int sz;

    if (!node) {
        if (valout) *valout = BONSAI_ERROR;
        return NULL;
    }

    left = GET(node->left);
    right = GET(node->right);
    sz = bonsai_size(left);
    if (index < sz)
        return balance(ctx, node, _bonsai_delete_index(ctx, left, index, valout), right, subtree_left, 1);
    if (index > sz)
        return balance(ctx, node, left, _bonsai_delete_index(ctx, right, index-(sz+1), valout), subtree_right, 1);

    if (valout) *valout = GET(node->value);
    if (!left) return right;
    if (!right) return left;
    right = delete_min(ctx, right, &min);
    return balance(ctx, min, left, right, subtree_right, 0);
}

dbtype_t *
bonsai_delete_index(pgctx_t *ctx, dbtype_t *node, int index, dbtype_t **value)
{
    dbtype_t *valout = NULL, *ret;
    int sz = bonsai_size(node);
    if (index < 0)
        index += sz;
    if (index < 0 || index >= sz)
        return BONSAI_ERROR;

    ret = _bonsai_delete_index(ctx, node, index, value);
    if (value) *value = valout;
    return (valout == BONSAI_ERROR) ? valout : ret;
}

dbtype_t *
bonsai_delete_value(pgctx_t *ctx, dbtype_t *node, dbtype_t *value)
{
    dbtype_t *min, *left, *right, *n;
    int cmp;

    if (!node) {
        return BONSAI_ERROR;
    }

    left = GET(node->left);
    right = GET(node->right);

    if (left) {
        n = bonsai_delete_value(ctx, left, value);
        if (n != BONSAI_ERROR) return balance(ctx, node, n, right, subtree_left, 1);
    }

    cmp = dbcmp(ctx, value, GET(node->value));
    if (cmp == 0) {
        if (!left) return right;
        if (!right) return left;
        right = delete_min(ctx, right, &min);
        return balance(ctx, min, left, right, subtree_right, 0);
    }

    if (right) {
        n = bonsai_delete_value(ctx, right, value);
        if (n != BONSAI_ERROR) return balance(ctx, node, left, n, subtree_right, 1);
    }
    return BONSAI_ERROR;
}

int
bonsai_find(pgctx_t *ctx, dbtype_t *node, dbtype_t *key, dbtype_t **value)
{
    int cmp;
    while(node) {
        cmp = dbcmp(ctx, key, GET(node->key));
        if (cmp < 0) {
            node = GET(node->left);
        } else if (cmp > 0) {
            node = GET(node->right);
        } else {
            break;
        }
    }

    if (node) {
        if (value) *value = GET(node->value);
        return 0;
    }
    return -1;
}

dbtype_t *
bonsai_find_primitive(pgctx_t *ctx, dbtype_t *node, dbtag_t type, const void *key)
{
    int cmp;
    while(node) {
        // This is subtle:  all other bonsai calls to dbcmp are of the 
        // form dbcmp(key, GET(node->key)).  However, dbcmp_primitive
        // only accepts dbtype for the first argument, so the sense of
        // the compare must be inverted:
        cmp = -dbcmp_primitive(GET(node->key), type, key);
        if (cmp < 0) {
            node = GET(node->left);
        } else if (cmp > 0) {
            node = GET(node->right);
        } else {
            return GET(node->value);
        }
    }
    return NULL;
}

int
bonsai_find_value(pgctx_t *ctx, dbtype_t *node, dbtype_t *value)
{
    int cmp;

    if (!node) return 0;

    cmp = bonsai_find_value(ctx, GET(node->left), value);
    if (cmp) return cmp;

    cmp = !dbcmp(ctx, GET(node->value), value);
    if (cmp) return cmp;

    cmp = bonsai_find_value(ctx, GET(node->right), value);
    return cmp;
}

dbtype_t *
bonsai_index(pgctx_t *ctx, dbtype_t *node, int index)
{
    int sz = bonsai_size(node);

    if (index < 0)
        index += sz;
    if (index < 0 || index >= sz)
        return NULL;

    while(node) {
        sz = bonsai_size(GET(node->left));
        if (index == sz) {
            break;
        } else if (index < sz) {
            node = GET(node->left);
        } else {
            index -= (sz + 1);
            node = GET(node->right);
        }
    }

    return node;
}

void
bonsai_foreach(pgctx_t *ctx, dbtype_t *node, bonsaicb_t cb, void *user)
{
    if (node) {
        bonsai_foreach(ctx, GET(node->left), cb, user);
        cb(ctx, node, user);
        bonsai_foreach(ctx, GET(node->right), cb, user);
    }
}

void
bonsai_show(pgctx_t *ctx, dbtype_t *node, int depth)
{
    char buf1[80], buf2[80];

    if (!node)
        return;

    bonsai_show(ctx, GET(node->left), depth+1);
    printf("%*s(%lld) %s=>%s\n",
            depth*2, "",
            node->size,
            dbprint(GET(node->key), buf1, sizeof(buf1)),
            dbprint(GET(node->value), buf2, sizeof(buf2)));
    bonsai_show(ctx, GET(node->right), depth+1);
}

// Bonsai tree as dictionary (key/value storage)
void test1(pgctx_t *ctx)
{
    int i;
    char kbuf[] = "a";
    char tbuf[80];
    dbtype_t *k[16], *v, *node, *n;

    node=NULL;
    for(i=0; i<16; i++) {
        kbuf[0] = 'a'+i;
        k[i] = dbstring_new(ctx, kbuf, -1);
        v = dbint_new(ctx, i);
        printf("inserting %s, node=%p ", dbprint(k[i], tbuf, sizeof(tbuf)), node);
        node = bonsai_insert(ctx, node, k[i], v, 0);
        printf("--> %p\n", node);
    }
    bonsai_show(ctx, node, 0);

    for(i=0; i<16; i++) {
        printf("deleting %s, node=%p ", dbprint(k[15-i], tbuf, sizeof(tbuf)), node);
        node = bonsai_delete(ctx, node, k[15-i], NULL);
        printf("--> %p\n", node);
        bonsai_show(ctx, node, 0);
        printf("**********************************************************************\n");
    }
    return;

    printf("aslist = [");
    for(i=0; i<16; i++) {
        n = bonsai_index(ctx, node, i);
        printf("%s, ", dbprint(GET(n->value), tbuf, sizeof(tbuf)));
    }
    printf("]\n\n");


    printf("deleting %s, node=%p ", dbprint(k[7], tbuf, sizeof(tbuf)), node);
    node = bonsai_delete(ctx, node, k[7], NULL);
    printf("--> %p\n", node);
    bonsai_show(ctx, node, 0);
    printf("aslist = [");
    for(i=0; i<15; i++) {
        n = bonsai_index(ctx, node, i);
        printf("%s, ", dbprint(GET(n->value), tbuf, sizeof(tbuf)));
    }
    printf("]\n\n");

    printf("deleting %s, node=%p ", dbprint(k[15], tbuf, sizeof(tbuf)), node);
    node = bonsai_delete(ctx, node, k[15], NULL);
    printf("--> %p\n", node);
    bonsai_show(ctx, node, 0);
    printf("aslist = [");
    for(i=0; i<14; i++) {
        n = bonsai_index(ctx, node, i);
        printf("%s, ", dbprint(GET(n->value), tbuf, sizeof(tbuf)));
    }
    printf("]\n\n");

}

// Bonsai tree as list (indexed storage)
void test2(pgctx_t *ctx)
{
    int i;
    char tbuf[80];
    dbtype_t *v, *node, *n;

    node=NULL;
    for(i=0; i<16; i++) {
        v = dbint_new(ctx, i);
        printf("inserting %d, node=%p ", i, node);
        node = bonsai_insert_index(ctx, node, INT_MAX, v);
        printf("--> %p\n", node);
    }
    bonsai_show(ctx, node, 0);

    printf("aslist = [");
    for(i=0; i<16; i++) {
        n = bonsai_index(ctx, node, i);
        printf("%s, ", dbprint(GET(n->value), tbuf, sizeof(tbuf)));
    }
    printf("]\n\n");

    for(i=0; i<16; i++) {
        v = NULL;
        printf("deleting %d, node=%p ", i, node);
        node = bonsai_delete_index(ctx, node, -1, &v);
        printf("--> %p (val=%s)\n", node, dbprint(v, tbuf, sizeof(tbuf)));
        bonsai_show(ctx, node, 0);
        printf("**********************************************************************\n");
    }
}
// vim: ts=4 sts=4 sw=4 expandtab:
