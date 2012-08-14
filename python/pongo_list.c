#include "pongo.h"
/*************************************************************************
 * PongoList Proxy implementation
 *
 * Present a python list-like interface to the underlying dblist type.
 ************************************************************************/

PyObject*
PongoList_Proxy(pgctx_t *ctx, dbtype_t *db)
{
    PongoList *self;

    self = (PongoList *)PyObject_New(PongoList, &PongoList_Type);
    if (!self)
        return NULL;

    self->ctx = ctx;
    self->dblist = _offset(ctx, db);
    return (PyObject *)self;
}

static PyObject*
PongoList_GetItem(PongoList *self, Py_ssize_t i)
{
    dbtype_t *item;
    PyObject *ret = NULL;

    dblock(self->ctx);
    if (i>=0 && dblist_getitem(SELF_CTX_AND_DBLIST, i, &item) == 0) {
        ret = to_python(self->ctx, item, 1);
    } else {
        PyErr_SetString(PyExc_IndexError, "list index out of range");
    }
    dbunlock(self->ctx);

    return ret;
}

static int
PongoList_SetItem(PongoList *self, Py_ssize_t i, PyObject *v)
{
    dbtype_t *item;
    int ret = -1;

    dblock(self->ctx);
    if (i>=0) {
        if (v == NULL) {
            ret = dblist_delitem(SELF_CTX_AND_DBLIST, i, &item, SYNC);
        } else {
            item = from_python(self->ctx, v);
            if (!PyErr_Occurred() && dblist_setitem(SELF_CTX_AND_DBLIST, i, item, SYNC) == 0) {
                ret = 0;
            }
        }
    }
    dbunlock(self->ctx);
    if (ret < 0)
        PyErr_SetString(PyExc_IndexError, "list index out of range");
    return ret;
}

static PyObject *
PongoList_append(PongoList *self, PyObject *args, PyObject *kwargs)
{
    PyObject *ret = NULL;
    PyObject *v;
    int sync = SYNC;
    char *kwlist[] = {"value", "sync", NULL};
    dbtype_t *item;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|i:append", kwlist,
                &v, &sync))
        return NULL;

    dblock(self->ctx);
    item = from_python(self->ctx, v);
    if (!PyErr_Occurred() && dblist_append(SELF_CTX_AND_DBLIST, item, sync) == 0) {
        ret = Py_None;
    }
    dbunlock(self->ctx);
    return ret;
}

static PyObject *
PongoList_insert(PongoList *self, PyObject *args, PyObject *kwargs)
{
    Py_ssize_t i;
    PyObject *v;
    int sync = SYNC;
    PyObject *ret = NULL;
    dbtype_t *item;
    char *kwlist[] = {"value", "sync", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "nO|i:insert", kwlist,
                &i, &v, &sync))
        return NULL;

    dblock(self->ctx);
    item = from_python(self->ctx, v);
    if (!PyErr_Occurred()) {
        if (dblist_insert(SELF_CTX_AND_DBLIST, i, item, sync) == 0) {
            ret = Py_None; Py_INCREF(ret);
        } else {
            PyErr_SetString(PyExc_IndexError, "list index out of range");
        }
    }
    dbunlock(self->ctx);
    return ret;
}

static PyObject *
PongoList_remove(PongoList *self, PyObject *args, PyObject *kwargs)
{
    PyObject *ret = NULL;
    dbtype_t *item;
    PyObject *v;
    int sync = SYNC;
    char *kwlist[] = {"value", "sync", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|i:remove", kwlist,
                &v, &sync))
        return NULL;
    dblock(self->ctx);
    item = from_python(self->ctx, v);
    if (!PyErr_Occurred()) {
        if (dblist_remove(SELF_CTX_AND_DBLIST, item, sync) == 0) {
            ret = Py_None; Py_INCREF(ret);
        } else {
            PyErr_SetString(PyExc_ValueError, "item not in list");
        }
    }
    dbunlock(self->ctx);
    return ret;
}

static PyObject *
PongoList_pop(PongoList *self, PyObject *args, PyObject *kwargs)
{
    Py_ssize_t i = -1;
    dbtype_t *item;
    PyObject *ret = NULL;
    int sync = SYNC;
    char *kwlist[] = {"n", "sync", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|ni:pop", kwlist,
                &i, &sync))
        return NULL;

    dblock(self->ctx);
    if (dblist_delitem(SELF_CTX_AND_DBLIST, i, &item, sync) == 0) {
        ret = to_python(self->ctx, item, 1);
    } else {
        PyErr_SetString(PyExc_IndexError, "list index out of range");
    }
    dbunlock(self->ctx);
    return ret;
}

static Py_ssize_t
PongoList_length(PongoList *self)
{
    dbtype_t *db;
    _list_t *list;
    int len;
    dblock(self->ctx);
    db = _ptr(self->ctx, self->dblist);
    list = _ptr(self->ctx, db->list);
    len = list->len;
    dbunlock(self->ctx);
    return len;
}

static int
PongoList_contains(PongoList *self, PyObject *elem)
{
    dbtype_t *item;
    int ret = 0;

    dblock(self->ctx);
    item = from_python(self->ctx, elem);
    if (!PyErr_Occurred()) {
        ret = dblist_contains(SELF_CTX_AND_DBLIST, item);
    }
    dbunlock(self->ctx);
    return ret;
}

static PyObject *
PongoList_native(PongoList *self)
{
    PyObject *ret;
    dblock(self->ctx);
    ret = to_python(SELF_CTX_AND_DBLIST, 0);
    dbunlock(self->ctx);
    return ret;
}

static PyObject *
PongoList_repr(PyObject *ob)
{
    PongoList *self = (PongoList*)ob;
    char buf[32];
    sprintf(buf, "0x%llx", self->dblist);
    return PyString_FromFormat("PongoList(%p, %s)", self->ctx, buf);
}

void PongoList_Del(PyObject *ob)
{
    PongoList *self = (PongoList*)ob;
    //printf("PongoList_Del %p %08llx\n", self, self->dblist);
    pidcache_del(self->ctx, self);
    PyObject_Del(ob);
}

static PyMethodDef pydblist_methods[] = {
    { "append", (PyCFunction)PongoList_append,       METH_VARARGS|METH_KEYWORDS, NULL },
    { "insert", (PyCFunction)PongoList_insert,       METH_VARARGS|METH_KEYWORDS, NULL },
    { "remove", (PyCFunction)PongoList_remove,       METH_VARARGS|METH_KEYWORDS, NULL },
    { "pop",    (PyCFunction)PongoList_pop,          METH_VARARGS|METH_KEYWORDS, NULL },
    { "native", (PyCFunction)PongoList_native,       METH_NOARGS, NULL },
    { NULL, NULL }
};

static PySequenceMethods pydblist_as_sequence = {
    (lenfunc)PongoList_length,           /* sq_length */
    NULL,                               /* sq_concat */
    NULL,                               /* sq_repeat */
    (ssizeargfunc)PongoList_GetItem,     /* sq_item */
    NULL,                               /* sq_slice */
    (ssizeobjargproc)PongoList_SetItem,  /* sq_ass_item */
    NULL,                               /* sq_ass_slice */
    (objobjproc)PongoList_contains,      /* sq_contains */
    NULL,                               /* sq_inplace_concat */
    NULL,                               /* sq_inplace_repeat */
};

PyTypeObject PongoList_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "_pongo.PongoList",         /*tp_name*/
    sizeof(PongoList), /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)PongoList_Del,  /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    PongoList_repr,             /*tp_repr*/
    0,                         /*tp_as_number*/
    &pydblist_as_sequence,     /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,        /*tp_flags*/
    "PongoDB List Proxy",           /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    pydblist_methods,          /* tp_methods */
    0,             /* tp_members */
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
