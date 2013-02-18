#include "pongo.h"
/*************************************************************************
 * PongoPointer Proxy implementation
 *
 * A python object representing a pointer in the pongo database.
 * 
 * This is a read-only object that should never be seen by the
 * end user.
 *
 * The only time it should be seen is when examining the pidcache
 * while a PongoIter is active.
 ************************************************************************/

PyObject*
PongoPointer_Proxy(pgctx_t *ctx, dbtype_t db)
{
    PongoPointer *self;

    self = (PongoPointer *)PyObject_New(PongoPointer, &PongoPointer_Type);
    if (!self)
        return NULL;

    self->ctx = ctx;
    self->dbptr = db;
    return (PyObject *)self;
}

static PyObject *
PongoPointer_repr(PyObject *ob)
{
    PongoPointer *self = (PongoPointer*)ob;
    char buf[32];
    sprintf(buf, "0x%" PRIx64, self->dbptr.all);
    return PyString_FromFormat("PongoPointer(%p, %s)", self->ctx, buf);
}

void PongoPointer_Del(PyObject *ob)
{
    PongoPointer *self = (PongoPointer*)ob;
    dblock(self->ctx);
    pidcache_del(self->ctx, self);
    dbunlock(self->ctx);
    PyObject_Del(ob);
}


PyTypeObject PongoPointer_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "_pongo.PongoPointer",         /*tp_name*/
    sizeof(PongoPointer), /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)PongoPointer_Del,  /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    PongoPointer_repr,             /*tp_repr*/
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
    "PongoDB Pointer Proxy",           /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    0,          /* tp_methods */
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
