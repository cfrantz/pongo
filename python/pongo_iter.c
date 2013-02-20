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
    int len;

    dblock(po->ctx);
    // Take advantage of the fact that all of the Pongo container types
    // have the same object layout of the first few fields.
    internal.ptr = dbptr(po->ctx, po->dbptr);
    if (internal.ptr->type == List) {
        internal = internal.ptr->list;
        list = dbptr(po->ctx, internal);
        len = list ? list->len : 0;
    } else if (internal.ptr->type == Object) {
        internal = internal.ptr->obj;
        obj = dbptr(po->ctx, internal);
        len = obj ? obj->len : 0;
    } else if (internal.ptr->type == Collection) {
        internal = internal.ptr->obj;
        node = dbptr(po->ctx, internal);
        len = node ? node->size : 0;
    } else if (internal.ptr->type == MultiCollection) {
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
    self->pos = 0;
    self->len = len;
    self->depth = -1;
    if (node) {
        self->stack[++self->depth] = internal;
    }

    pidcache_put(po->ctx, self, self->dbptr);

exitproc:
    dbunlock(self->ctx);
    return (PyObject *)self;
}

static PyObject *
PongoIter_next(PyObject *ob)
{
    PongoIter *self = (PongoIter*)ob;
    dbval_t *internal, *pn;
    dbtype_t node;
    PyObject *ret=NULL, *k, *v;
    _list_t *list;
    _obj_t *obj;

    dblock(self->ctx);
    internal = dbptr(self->ctx, self->dbptr);
    if (internal->type == _InternalList) {
        list = (_list_t*)internal;
        if (self->pos < self->len) {
            ret = to_python(self->ctx, list->item[self->pos], TP_PROXY);
            self->pos++;
        } else {
            PyErr_SetNone(PyExc_StopIteration);
        }
    } else if (internal->type == _InternalObj) {
        obj = (_obj_t*)internal;
        if (self->pos < self->len) {
            k = to_python(self->ctx, obj->item[self->pos].key, TP_PROXY);
            v = to_python(self->ctx, obj->item[self->pos].value, TP_PROXY);
            ret = PyTuple_Pack(2, k, v);
            self->pos++;
        } else {
            PyErr_SetNone(PyExc_StopIteration);
        }
    } else if (internal->type == _BonsaiNode || internal->type == _BonsaiMultiNode) {
        if (self->depth == -1) {
            PyErr_SetNone(PyExc_StopIteration);
        } else {
            // NOTE: I'm overloading the lowest bit to mean "already traversed the left
            // side".  Normally, this bit is part of the 'type' field and would encode
            // this value as "Int".  However, the BonsaiNode left/right can only point
            // to other BonsaiNodes and the stack (where the bit overloading is happening)
            // lives only in main memory, so we'll never write this bit modification
            // to disk.

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
PongoIter_repr(PyObject *ob)
{
    PongoIter *self = (PongoIter*)ob;
    char buf[32];
    sprintf(buf, "0x%" PRIx64, self->dbptr.all);
    return PyString_FromFormat("PongoIter(%p, %s)", self->ctx, buf);
}

void PongoIter_Del(PyObject *ob)
{
    PongoIter *self = (PongoIter*)ob;
    dblock(self->ctx);
    pidcache_del(self->ctx, self);
    dbunlock(self->ctx);
    PyObject_Del(ob);
}


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
    0,                         /* tp_methods */
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
