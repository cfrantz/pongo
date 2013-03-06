#include "pongo.h"
/*************************************************************************
 * PongoIter implementation
 *
 * A python object representing an iterator over a PongoList, PongoDict
 * or PongoCollection
 *
 * PongoIters keep a reference to the internal list, dict or tree
 * when they are created, so it is safe to mutate the object while
 * iterating.  The iterator will not see the mutations -- it will
 * see the {list,dict,tree} as it was when the iterator was created.
 * 
 ************************************************************************/

PyObject*
PongoIter_Iter(PyObject *ob)
{
    PongoDict *po = (PongoDict*)ob;
    PongoIter *self = NULL;
    dbtype_t internal;
    _list_t *list = NULL;
    _obj_t *obj = NULL;
    dbnode_t *node = NULL;
    dbtag_t tag;
    int len;

    dblock(po->ctx);
    // Take advantage of the fact that all of the Pongo container types
    // have the same object layout of the first few fields.
    internal.ptr = dbptr(po->ctx, po->dbptr);
    tag = internal.ptr->type;
    if (tag == List) {
        internal = internal.ptr->list;
        list = dbptr(po->ctx, internal);
        len = list ? list->len : 0;
    } else if (tag == Object) {
        internal = internal.ptr->obj;
        obj = dbptr(po->ctx, internal);
        len = obj ? obj->len : 0;
    } else if (tag == Collection) {
        internal = internal.ptr->obj;
        node = dbptr(po->ctx, internal);
        len = node ? node->size : 0;
    } else if (tag == MultiCollection) {
        internal = internal.ptr->obj;
        node = dbptr(po->ctx, internal);
        len = node ? node->size : 0;
    } else {
        goto exitproc;
    }

    self = (PongoIter *)PyObject_New(PongoIter, &PongoIter_Type);
    if (!self)
        goto exitproc;

    self->ctx = po->ctx;
    self->dbptr = internal;
    self->tag = tag;
    self->pos = 0;
    self->len = len;
    self->depth = -1;
    self->lhex = 0;
    self->rhex = 0;
    self->lhdata = self->rhdata = DBNULL;
    pidcache_put(po->ctx, self, self->dbptr);

exitproc:
    dbunlock(self->ctx);
    return (PyObject *)self;
}

static PyObject *
PongoIter_next_item(PyObject *ob)
{
    PongoIter *self = (PongoIter*)ob;
    dbval_t *internal, *pn;
    dbtype_t node;
    PyObject *ret=NULL, *k, *v;
    _list_t *list;
    _obj_t *obj;

    dblock(self->ctx);
    internal = dbptr(self->ctx, self->dbptr);
    if (!internal) {
        // The {List,Object,Collection} internal pointer is NULL, so
        // there is no iteration to do
        PyErr_SetNone(PyExc_StopIteration);
    } else if (self->tag == List) {
        list = (_list_t*)internal;
        if (self->pos < self->len) {
            ret = to_python(self->ctx, list->item[self->pos], TP_PROXY);
            self->pos++;
        } else {
            PyErr_SetNone(PyExc_StopIteration);
        }
    } else if (self->tag == Object) {
        obj = (_obj_t*)internal;
        if (self->pos < self->len) {
            k = to_python(self->ctx, obj->item[self->pos].key, TP_PROXY);
            v = to_python(self->ctx, obj->item[self->pos].value, TP_PROXY);
            ret = PyTuple_Pack(2, k, v);
            self->pos++;
        } else {
            PyErr_SetNone(PyExc_StopIteration);
        }
    } else if (self->tag == Collection || self->tag == MultiCollection) {
        if (self->pos && self->depth == -1) {
            PyErr_SetNone(PyExc_StopIteration);
        } else {
            // NOTE: I'm overloading the lowest bit to mean "already traversed the left
            // side".  Normally, this bit is part of the 'type' field and would encode
            // this value as "Int".  However, the BonsaiNode left/right can only point
            // to other BonsaiNodes and the stack (where the bit overloading is happening)
            // lives only in main memory, so we'll never write this bit modification
            // to disk.

            // If were at position 0, put the internal node onto the stack
            // I'm reusing pos as a flag since pos is otherwise unused by the tree iterator
            if (self->pos == 0) {
                self->stack[++self->depth] = self->dbptr;
                self->pos = 1;
            }

            // Get current top of stack.  If we've already traversed the left side
            // of this node, go directly to the emit stage and traverse the right side.
            node = self->stack[self->depth];
            pn = _ptr(self->ctx, node.all & ~1);
            if (node.all & 1) {
                node.all &= ~1;
            } else {
                // Walk as far left as possible, pushing onto stack as we
                // follow each link
                while (pn->left.all) {
                    node = pn->left;
                    self->stack[++self->depth] = node;
                    pn = _ptr(self->ctx, node.all);
                }
            }
            // Now node, pn and top of stack all reference the same object,
            // so convert the object to python, pop the top of stack and
            // mark the new top as "left side traversed"
            ret = to_python(self->ctx, node, TP_NODEKEY|TP_NODEVAL|TP_PROXY);
            if (--self->depth >= 0) {
                self->stack[self->depth].all |= 1;
            }

            // Now check if there is a right branch in the tree and push
            // it for the next call to the iterator
            if (pn->right.all) {
                self->stack[++self->depth] = pn->right;
            }
        }
    }
    dbunlock(self->ctx);
    return ret;
}


static PyObject *
PongoIter_next_expr(PyObject *ob)
{
    PongoIter *self = (PongoIter*)ob;
    dbval_t *internal, *pn;
    dbtype_t node, key, val;
    PyObject *ret=NULL, *k, *v;
    int ls, rs;
    _list_t *list;
    _obj_t *obj;

    dblock(self->ctx);
    internal = dbptr(self->ctx, self->dbptr);
    if (!internal) {
        PyErr_SetNone(PyExc_StopIteration);
    } else if (self->tag == List) {
        list = (_list_t*)internal;
        for(;;) {
            if (self->pos == self->len) {
                PyErr_SetNone(PyExc_StopIteration);
                break;
            }
            node = list->item[self->pos++];
            if ((!self->lhdata.all || dbcmp(self->ctx, node, self->lhdata) >= self->lhex) &&
                (!self->rhdata.all || dbcmp(self->ctx, node, self->rhdata) <= self->rhex)) {
                    ret = to_python(self->ctx, node, TP_PROXY);
                    break;
            }
        }
    } else if (self->tag == Object) {
        obj = (_obj_t*)internal;
        for(;;) {
            // If we've reached the end, quit with StopIteration
            if (self->pos == self->len) {
                PyErr_SetNone(PyExc_StopIteration);
                break;
            }
            key = obj->item[self->pos].key;
            val = obj->item[self->pos].value;
            self->pos++;
            // If the key doesn't satisfy the RHS, and since Objects are
            // sorted, we can quit with StopIteration
            if (!(!self->rhdata.all || dbcmp(self->ctx, key, self->rhdata) <= self->rhex)) {
                PyErr_SetNone(PyExc_StopIteration);
                break;
            }
            // If the key does satisfy the LHS, return it
            if (!self->lhdata.all || dbcmp(self->ctx, key, self->lhdata) >= self->lhex) {
                k = to_python(self->ctx, key, TP_PROXY);
                v = to_python(self->ctx, val, TP_PROXY);
                ret = PyTuple_Pack(2, k, v);
                break;
            }
        }
    } else if (self->tag == Collection || self->tag == MultiCollection) {
        if (self->pos && self->depth == -1) {
            PyErr_SetNone(PyExc_StopIteration);
        } else {
            // NOTE: I'm overloading the lowest bit to mean "already traversed the left
            // side".  Normally, this bit is part of the 'type' field and would encode
            // this value as "Int".  However, the BonsaiNode left/right can only point
            // to other BonsaiNodes and the stack (where the bit overloading is happening)
            // lives only in main memory, so we'll never write this bit modification
            // to disk.

            // If were at position 0, put the internal node onto the stack
            // I'm reusing pos as a flag since pos is otherwise unused by the tree iterator
            if (self->pos == 0) {
                node = self->dbptr;
                for(;;) {
                    pn = _ptr(self->ctx, node.all);
                    ls = (!self->lhdata.all || dbcmp(self->ctx, pn->key, self->lhdata) >= self->lhex);
                    rs = (!self->rhdata.all || dbcmp(self->ctx, pn->key, self->rhdata) <= self->rhex);
                    if (ls && rs) {
                        self->stack[++self->depth] = node;
                        self->pos = 1;
                        break;
                    } else if (ls && pn->left.all) {
                        node = pn->left;
                    } else if (rs && pn->right.all) {
                        node = pn->right;
                    } else {
                        PyErr_SetNone(PyExc_StopIteration);
                        goto exitproc;
                    }
                }
            }

            // Get current top of stack.  If we've already traversed the left side
            // of this node, go directly to the emit stage and traverse the right side.
            node = self->stack[self->depth];
            pn = _ptr(self->ctx, node.all & ~1);
            if (node.all & 1) {
                node.all &= ~1;
            } else {
                // Walk as far left as possible, pushing onto stack as we
                // follow each link
                if (pn->left.all) {
                    node = pn->left;
                    for(;;) {
                        pn = _ptr(self->ctx, node.all);
                        ls = (!self->lhdata.all || dbcmp(self->ctx, pn->key, self->lhdata) >= self->lhex);
                        rs = (!self->rhdata.all || dbcmp(self->ctx, pn->key, self->rhdata) <= self->rhex);
                        if (ls && rs) {
                            self->stack[++self->depth] = node;
                        }
                        if (ls && pn->left.all) {
                            node = pn->left;
                        } else if (rs && pn->right.all) {
                            node = pn->right;
                        } else {
                            break;
                        }
                    }
                    // Reset node and pn to whatever is on the top of stack now
                    node = self->stack[self->depth];
                    pn = _ptr(self->ctx, node.all);
                }
            }
            // Now node, pn and top of stack all reference the same object,
            // so convert the object to python, pop the top of stack and
            // mark the new top as "left side traversed"
            ret = to_python(self->ctx, node, TP_NODEKEY|TP_NODEVAL|TP_PROXY);
            if (--self->depth >= 0) {
                self->stack[self->depth].all |= 1;
            }

            // Now check if there is a right branch in the tree and push
            // it for the next call to the iterator
            if (pn->right.all) {
                node = pn->right;
                for(;;) {
                    pn = _ptr(self->ctx, node.all);
                    ls = (!self->lhdata.all || dbcmp(self->ctx, pn->key, self->lhdata) >= self->lhex);
                    rs = (!self->rhdata.all || dbcmp(self->ctx, pn->key, self->rhdata) <= self->rhex);
                    if (ls && rs) {
                        self->stack[++self->depth] = node;
                    }
                    if (ls && pn->left.all) {
                        node = pn->left;
                    } else if (rs && pn->right.all) {
                        node = pn->right;
                    } else {
                        break;
                    }
                }
            }
        }
    }
exitproc:
    dbunlock(self->ctx);
    return ret;
}

static PyObject *
PongoIter_next(PyObject *ob)
{
    PongoIter *self = (PongoIter*)ob;
    PyObject *ret;
    if (self->lhdata.all || self->rhdata.all)
        ret = PongoIter_next_expr(ob);
    else
        ret = PongoIter_next_item(ob);
    return ret;
}

PyDoc_STRVAR(expr_doc,
"x.expr(lhs, rhs, lhex, rhex) -- Filter the iterator such that\n"
"the returned items match the expression lhs <= item <= rhs.\n"
"\n"
"lhex and rhex determine whether each side of the expression is\n"
"exlcusive (<) or inclusive (<=).  Default inclusive.");
static PyObject *
PongoIter_expr(PyObject *ob, PyObject *args, PyObject *kwargs)
{
    PongoIter *self = (PongoIter*)ob;
    PyObject *lhs = Py_None, *rhs = Py_None;
    int lhex = 0, rhex = 0;
    char *kwlist[] = {"lhs", "rhs", "lhex", "rhex", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|OOii:expr", kwlist,
                &lhs, &rhs, &lhex, &rhex))
        return NULL;

    dblock(self->ctx);
    // Force lhex to be 0 or 1, and rhex to be 0 or -1
    self->lhex = !!lhex;
    self->rhex = -(!!rhex);

    // Convert the lhs and rhs to pongo objects and reference them in the pidcache
    self->lhdata = from_python(self->ctx, lhs);
    if (self->lhdata.all)
        pidcache_put(self->ctx, &self->lhdata, self->lhdata);

    self->rhdata = from_python(self->ctx, rhs);
    if (self->rhdata.all)
        pidcache_put(self->ctx, &self->rhdata, self->rhdata);
    dbunlock(self->ctx);
    Py_INCREF(self);
    return (PyObject*)self;
}

static PyObject *
PongoIter_repr(PyObject *ob)
{
    PongoIter *self = (PongoIter*)ob;
    char buf[32];
    sprintf(buf, "0x%" PRIx64, self->dbptr.all);
    return PyString_FromFormat("PongoIter(%p, %s, tag=0x%x)", self->ctx, buf, self->tag);
}

void PongoIter_Del(PyObject *ob)
{
    PongoIter *self = (PongoIter*)ob;
    dblock(self->ctx);
    pidcache_del(self->ctx, self);
    pidcache_del(self->ctx, &self->lhdata);
    pidcache_del(self->ctx, &self->rhdata);
    dbunlock(self->ctx);
    PyObject_Del(ob);
}

static PyMethodDef iter_methods[] = {
    { "expr", (PyCFunction)PongoIter_expr, METH_VARARGS|METH_KEYWORDS, expr_doc },
    { NULL }
};

PyTypeObject PongoIter_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "_pongo.PongoIter",         /*tp_name*/
    sizeof(PongoIter), /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)PongoIter_Del,  /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    PongoIter_repr,             /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,        /*tp_flags*/
    "PongoDB Iter Proxy",           /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    PyObject_SelfIter,         /* tp_iter */
    PongoIter_next,            /* tp_iternext */
    iter_methods,              /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    0,                         /* tp_init */
    0,                         /* tp_alloc */
    0,                         /* tp_new */
};

// vim: ts=4 sts=4 sw=4 expandtab:
