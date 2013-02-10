#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <pongo/dbmem.h>
#include <pongo/context.h>
#include <pongo/dbtypes.h>
#include <pongo/bonsai.h>


// A little shortcut for all the silly ctx pointer/offset conversions
#define GET(x) ((dbtype_t*)_ptr(ctx, x))
#define WEIGHT 4
//#define rcuwinner(a, x) dbfree(a, x)
#define rcuwinner(a, x) do { assert(ctx->winner.len < 126); ctx->winner.addr[ctx->winner.len++] = a; } while(0)
#define rculoser(a, x)  do { assert(ctx->loser.len < 126); ctx->loser.addr[ctx->loser.len++] = a; } while(0)

        

typedef enum {
    subtree_left, subtree_right
} subtree_t;

int
bonsai_size(pgctx_t *ctx, dbtype_t node)
{
    if (node.all == 0)
        return 0;
    node.ptr = dbptr(ctx, node);
    return node.ptr->size;
}

static dbtype_t
bonsai_new(pgctx_t *ctx, dbtype_t left, dbtype_t right, dbtype_t key, dbtype_t value)
{
    dbtype_t node;

    node.ptr = dballoc(ctx, sizeof(dbnode_t));
    node.ptr->type = _BonsaiNode;
    node.ptr->left = left;
    node.ptr->right = right;
    node.ptr->size = 1 + bonsai_size(ctx, left) + bonsai_size(ctx, right);
    node.ptr->key = key;
    node.ptr->value = value;
    rculoser(node, 0);
    return dboffset(ctx, node.ptr);
}

static dbtype_t
bonsai_multi_new(pgctx_t *ctx, dbtype_t left, dbtype_t right, dbtype_t key, dbtype_t value)
{
    dbtype_t node;

    node.ptr = dballoc(ctx, sizeof(dbmultinode_t) + sizeof(dbtype_t));
    node.ptr->type = _BonsaiMultiNode;
    node.ptr->left = left;
    node.ptr->right = right;
    node.ptr->size = 1 + bonsai_size(ctx, left) + bonsai_size(ctx, right);
    node.ptr->key = key;
    node.ptr->nvalue = 1;
    node.ptr->values[0] = value;
    rculoser(node, 0);
    return dboffset(ctx, node.ptr);
}

static dbtype_t
bonsai_ncopy(pgctx_t *ctx, dbtype_t left, dbtype_t right, dbtype_t orig, int n)
{
    dbtype_t node;
    unsigned copysz = 0;

    orig.ptr = dbptr(ctx, orig);
    if (orig.ptr->type == _BonsaiNode) {
        node.ptr = dballoc(ctx, sizeof(dbnode_t));
        copysz = 2*sizeof(dbtype_t);
    } else if (orig.ptr->type == _BonsaiMultiNode) {
        node.ptr = dballoc(ctx, sizeof(dbmultinode_t) + orig.ptr->nvalue * sizeof(dbtype_t));
        copysz = 2*sizeof(dbtype_t) + (orig.ptr->nvalue + n) * sizeof(dbtype_t);
    }
    node.ptr->type = orig.ptr->type;
    node.ptr->left = left;
    node.ptr->right = right;
    node.ptr->size = 1 + bonsai_size(ctx, left) + bonsai_size(ctx, right);
    memcpy(&node.ptr->key, &orig.ptr->key, copysz);
    rculoser(node, 0);
    return dboffset(ctx, node.ptr);
}

static dbtype_t
bonsai_copy(pgctx_t *ctx, dbtype_t left, dbtype_t right, dbtype_t orig)
{
    return bonsai_ncopy(ctx, left, right, orig, 0);
}

static dbtype_t
single_left(pgctx_t *ctx, dbtype_t left, dbtype_t right, dbtype_t orig)
{
    dbtype_t ret;
    dbval_t *r = dbptr(ctx, right);

    ret = bonsai_copy(ctx,
            bonsai_copy(ctx, left, r->left, orig),
            r->right,
            right);
    rcuwinner(right, 0xe1);
    return ret;
}

static dbtype_t 
double_left(pgctx_t *ctx, dbtype_t left, dbtype_t right, dbtype_t orig)
{
    dbtype_t ret;
    dbval_t *r = dbptr(ctx, right);
    dbval_t *rl = dbptr(ctx, r->left);

    ret = bonsai_copy(ctx,
            bonsai_copy(ctx, left, rl->left, orig),
            bonsai_copy(ctx, rl->right, r->right, right),
                    r->left);
    rcuwinner(r->left, 0xe2);
    rcuwinner(right, 0xe3);
    return ret;
}

static dbtype_t
single_right(pgctx_t *ctx, dbtype_t left, dbtype_t right, dbtype_t orig)
{
    dbtype_t ret;
    dbval_t *l = dbptr(ctx, left);
    ret = bonsai_copy(ctx,
            l->left,
            bonsai_copy(ctx, l->right, right, orig),
            left);
    rcuwinner(left, 0xe4);
    return ret;
}

static dbtype_t
double_right(pgctx_t *ctx, dbtype_t left, dbtype_t right, dbtype_t orig)
{
    dbtype_t ret;
    dbval_t *l = dbptr(ctx, left);
    dbval_t *lr = dbptr(ctx, l->right);

    ret = bonsai_copy(ctx,
            bonsai_copy(ctx, l->left, lr->left, left),
            bonsai_copy(ctx, lr->right, right, orig),
            l->right);
    rcuwinner(l->right, 0xe5);
    rcuwinner(left, 0xe6);
    return ret;
}

static dbtype_t
balance_left(pgctx_t *ctx, dbtype_t left, dbtype_t right, dbtype_t orig)
{
    dbval_t *r = dbptr(ctx, right);
    uint64_t rln = bonsai_size(ctx, r->left);
    uint64_t rrn = bonsai_size(ctx, r->right);
    return (rln < rrn) ?
        single_left(ctx, left, right, orig) :
        double_left(ctx, left, right, orig) ;
}

static dbtype_t
balance_right(pgctx_t *ctx, dbtype_t left, dbtype_t right, dbtype_t orig)
{ 
    dbval_t *l = dbptr(ctx, left);
    uint64_t lln = bonsai_size(ctx, l->left);
    uint64_t lrn = bonsai_size(ctx, l->right);
    return (lrn < lln) ?
        single_right(ctx, left, right, orig) :
        double_right(ctx, left, right, orig) ;
}

static inline dbtype_t
balance(pgctx_t *ctx, dbtype_t cur, dbtype_t left, dbtype_t right, subtree_t which, int inplace)
{
    uint64_t ln = bonsai_size(ctx, left);
    uint64_t rn = bonsai_size(ctx, right);
    dbval_t *cp = dbptr(ctx, cur);
    dbtype_t ret;

    if (ln+rn < 2)
        goto balanced;
    if (rn > WEIGHT * ln)
        ret = balance_left(ctx, left, right, cur);
    else if (ln > WEIGHT * rn)
        ret = balance_right(ctx, left, right, cur);
    else
        goto balanced;

    rcuwinner(cur, 0xe7);
    return ret;
balanced:
    inplace = 0;
    if (inplace) {
        if (which == subtree_left) {
            // rcu_assign_pointer
            cp->left = left;
        } else {
            // rcu_assign_pointer
            cp->right = right;
        }
        cp->size = 1 + bonsai_size(ctx, left) + bonsai_size(ctx, right);
        ret = cur;
    } else {
        ret = bonsai_copy(ctx, left, right, cur);
        rcuwinner(cur, 0xe8);
    }
    return ret;
}

dbtype_t
bonsai_insert(pgctx_t *ctx, dbtype_t node, dbtype_t key, dbtype_t value, int insert_or_fail)
{
    int cmp;
    dbval_t *np;
    if (!node.all) {
        return bonsai_new(ctx, DBNULL, DBNULL, key, value);
    }

    np = dbptr(ctx, node);
    cmp = dbcmp(ctx, key, np->key);
    if (insert_or_fail && cmp==0) {
        node.type = Error;
        return node;
    }
    if (cmp < 0) {
        return balance(ctx, node,
                bonsai_insert(ctx, np->left, key, value, insert_or_fail),
                np->right,
                subtree_left, 1);
    }
    if (cmp > 0) {
        return balance(ctx, node,
                np->left,
                bonsai_insert(ctx, np->right, key, value, insert_or_fail),
                subtree_right, 1);
    }
    np->value = value;
    return node;
}

dbtype_t
bonsai_multi_insert(pgctx_t *ctx, dbtype_t node, dbtype_t key, dbtype_t value)
{
    int cmp;
    int i, j, done;
    dbval_t *np, *orig;
    if (!node.all) {
        return bonsai_multi_new(ctx, DBNULL, DBNULL, key, value);
    }

    np = dbptr(ctx, node);
    cmp = dbcmp(ctx, key, np->key);
    if (cmp < 0) {
        return balance(ctx, node,
                bonsai_multi_insert(ctx, np->left, key, value),
                np->right,
                subtree_left, 1);
    }
    if (cmp > 0) {
        return balance(ctx, node,
                np->left,
                bonsai_multi_insert(ctx, np->right, key, value),
                subtree_right, 1);
    }
    orig = np;
    node = bonsai_ncopy(ctx, np->left, np->right, node, 1);
    np = dbptr(ctx, node);
    for(done=i=j=0; i<orig->nvalue; i++, j++) {
        if (!done) {
            cmp = dbcmp(ctx, value, orig->values[i]);
            if (cmp < 0) {
                np->values[j++] = value;
                np->nvalue++;
                done = 1;
            } else if (cmp == 0) {
                done = 1;
            }
        }
        np->values[j] = orig->values[i];
    }
    if (!done) {
        np->values[j++] = value;
        np->nvalue++;
    }

    return node;
}

static dbtype_t 
delete_min(pgctx_t *ctx, dbtype_t node, dbtype_t *out)
{
    dbval_t *np = dbptr(ctx, node);
    dbtype_t left = np->left, right = np->right;

    if (!left.all) {
        *out = node;
        return right;
    }
    return balance(ctx, node, delete_min(ctx, left, out), right, subtree_left, 0);
}

static dbtype_t
_bonsai_delete(pgctx_t *ctx, dbtype_t node, dbtype_t key, dbtype_t *valout)
{
    dbval_t *np = dbptr(ctx, node);
    dbtype_t min, left, right;
    int cmp;

    if (!node.all) {
        if (valout) valout->type = Error;
        return DBNULL;
    }

    left = np->left;
    right = np->right;
    cmp = dbcmp(ctx, key, np->key);
    if (cmp < 0) {
        return balance(ctx, node, _bonsai_delete(ctx, left, key, valout), right, subtree_left, 1);
    }
    if (cmp > 0) {
        return balance(ctx, node, left, _bonsai_delete(ctx, right, key, valout), subtree_right, 1);
    }

    if (valout) *valout = np->value;
    rcuwinner(node, 0xe9);

    if (!left.all) return right;
    if (!right.all) return left;
    right = delete_min(ctx, right, &min);
    return balance(ctx, min, left, right, subtree_right, 0);
}

dbtype_t
bonsai_delete(pgctx_t *ctx, dbtype_t node, dbtype_t key, dbtype_t *value)
{
    dbtype_t valout = DBNULL;
    dbtype_t ret = _bonsai_delete(ctx, node, key, &valout);
    if (value) *value = valout;
    return (valout.type == Error) ? valout : ret;
}

static dbtype_t
_bonsai_multi_delete(pgctx_t *ctx, dbtype_t node, dbtype_t key, dbtype_t *value)
{
    dbval_t *np = dbptr(ctx, node);
    dbtype_t min, left, right;
    int cmp;
    int i;

    if (!node.all) {
        value->type = Error;
        return DBNULL;
    }

    left = np->left;
    right = np->right;
    cmp = dbcmp(ctx, key, np->key);
    if (cmp < 0) {
        return balance(ctx, node, _bonsai_delete(ctx, left, key, value), right, subtree_left, 1);
    }
    if (cmp > 0) {
        return balance(ctx, node, left, _bonsai_delete(ctx, right, key, value), subtree_right, 1);
    }

    if (np->nvalue == 1) {
        if (dbcmp(ctx, *value, np->values[0])!=0) {
            value->type = Error;
            return node;
        }
        if (!left.all) return right;
        if (!right.all) return left;
        right = delete_min(ctx, right, &min);
        rcuwinner(node, 0xe9);
        return balance(ctx, min, left, right, subtree_right, 0);
    }

    node = bonsai_copy(ctx, left, right, node);
    np = dbptr(ctx, node);
    for(i=0; i<np->nvalue; i++) {
        if (dbcmp(ctx, *value, np->values[i])==0) {
            memcpy(np->values+i, np->values+i+1, (np->nvalue-i-1)*sizeof(dbtype_t));
            break;
        }
    }
    if (i == np->nvalue) {
        value->type = Error;
    } else {
        np->nvalue--;
    }
    return node;
}

dbtype_t
bonsai_multi_delete(pgctx_t *ctx, dbtype_t node, dbtype_t key, dbtype_t value)
{
    dbtype_t ret = _bonsai_multi_delete(ctx, node, key, &value);
    return (value.type == Error) ? value : ret;
}

int
bonsai_find(pgctx_t *ctx, dbtype_t node, dbtype_t key, dbtype_t *value)
{
    int cmp;
    while(node.all) {
        node.ptr = dbptr(ctx, node);
        cmp = dbcmp(ctx, key, node.ptr->key);
        if (cmp < 0) {
            node = node.ptr->left;
        } else if (cmp > 0) {
            node = node.ptr->right;
        } else {
            if (value) *value = node.ptr->value;
            return 0;
        }
    }
    return -1;
}

dbtype_t
bonsai_find_node(pgctx_t *ctx, dbtype_t node, dbtype_t key)
{
    int cmp;
    dbtype_t ret;
    while(node.all) {
        ret = node;
        node.ptr = dbptr(ctx, node);
        cmp = dbcmp(ctx, key, node.ptr->key);
        if (cmp < 0) {
            node = node.ptr->left;
        } else if (cmp > 0) {
            node = node.ptr->right;
        } else {
            return ret;
        }
    }
    return DBNULL;
}

dbtype_t
bonsai_find_primitive(pgctx_t *ctx, dbtype_t node, dbtag_t type, const void *key)
{
    int cmp;
    while(node.all) {
        node.ptr = dbptr(ctx, node);
        // This is subtle:  all other bonsai calls to dbcmp are of the 
        // form dbcmp(key, GET(node->key)).  However, dbcmp_primitive
        // only accepts dbtype for the first argument, so the sense of
        // the compare must be inverted:
        cmp = -dbcmp_primitive(ctx, node.ptr->key, type, key);
        if (cmp < 0) {
            node = node.ptr->left;
        } else if (cmp > 0) {
            node = node.ptr->right;
        } else {
            return node.ptr->value;
        }
    }
    return DBNULL;
}

void
bonsai_foreach(pgctx_t *ctx, dbtype_t node, bonsaicb_t cb, void *user)
{
    dbval_t *np;
    if (node.all) {
        np = dbptr(ctx, node);
        bonsai_foreach(ctx, np->left, cb, user);
        cb(ctx, node, user);
        bonsai_foreach(ctx, np->right, cb, user);
    }
}

void
bonsai_show(pgctx_t *ctx, dbtype_t node, int depth)
{
    char buf1[80], buf2[80];
    dbval_t *np;

    if (!node.all)
        return;

    np = dbptr(ctx, node);
    bonsai_show(ctx, np->left, depth+1);
    printf("%*s(%" PRId64 ") %s=>%s\n",
            depth*2, "",
            np->size,
            dbprint(ctx, np->key, buf1, sizeof(buf1)),
            dbprint(ctx, np->value, buf2, sizeof(buf2)));
    bonsai_show(ctx, np->right, depth+1);
}

#if 0
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
#endif
// vim: ts=4 sts=4 sw=4 expandtab:

