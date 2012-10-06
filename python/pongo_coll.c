#include "pongo.h"
#include <pongo/bonsai.h>

/*************************************************************************
 * PongoCollection Proxy implementation
 *
 * Present a python dict-like interface to the underlying dbcollection type.
 ************************************************************************/

PyObject*
PongoCollection_Proxy(pgctx_t *ctx, dbtype_t *db)
{
    PongoCollection *self;

    self = (PongoCollection *)PyObject_New(PongoCollection, &PongoCollection_Type);
    if (!self)
        return NULL;

    self->ctx = ctx;
    self->dbcoll = _offset(ctx, db);
    return (PyObject *)self;
}

static PyObject *
PongoCollection_GetItem(PongoCollection *self, PyObject *key)
{
    dbtype_t *k, *v;
    PyObject *ret = NULL;

    dblock(self->ctx);
    k = from_python(self->ctx, key);
    if (!PyErr_Occurred()) {
        if (dbcollection_getitem(SELF_CTX_AND_DBCOLL, k, &v) == 0) {
            ret = to_python(self->ctx, v, 1);
        } else {
            PyErr_SetObject(PyExc_KeyError, key);
        }
    }
    dbunlock(self->ctx);
    return ret;
}

static int
PongoCollection_SetItem(PongoCollection *self, PyObject *key, PyObject *value)
{
    dbtype_t *k;
    dbtype_t *v;
    int ret = -1;

    dblock(self->ctx);
    k = from_python(self->ctx, key);
    if (!PyErr_Occurred()) {
        if (value == NULL) {
            if (dbcollection_delitem(SELF_CTX_AND_DBCOLL, k, &v, self->ctx->sync) == 0) {
                ret = 0;
            } else {
                PyErr_SetObject(PyExc_KeyError, key);
            }
        } else {
            v = from_python(self->ctx, value);
            if (!PyErr_Occurred() && dbcollection_setitem(SELF_CTX_AND_DBCOLL, k, v, self->ctx->sync) == 0)
                ret = 0;
        }
    }
    dbunlock(self->ctx);
    return ret;
}

static int
PongoCollection_length(PongoCollection *self)
{
    int len;

    dblock(self->ctx);
    len = dbcollection_len(SELF_CTX_AND_DBCOLL);
    dbunlock(self->ctx);
    return len;
}

static int
PongoCollection_contains(PongoCollection *self, PyObject *key)
{
    dbtype_t *k;
    int ret = 0;

    dblock(self->ctx);
    k = from_python(self->ctx, key);
    if (!PyErr_Occurred()) {
        ret = dbcollection_contains(SELF_CTX_AND_DBCOLL, k);
    }
    dbunlock(self->ctx);
    return ret;
}

static PyObject *
PongoCollection_get(PongoCollection *self, PyObject *args, PyObject *kwargs)
{
    PyObject *key, *dflt = Py_None;
    PyObject *klist = NULL;
    PyObject *ret = NULL;
    dbtype_t *k, *v;
    char *sep = ".";
    char *kwlist[] = {"key", "default", "sep", NULL};
    int r;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|Os:get", kwlist,
                &key, &dflt, &sep))
        return NULL;

    dblock(self->ctx);
    if (PyString_Check(key) || PyUnicode_Check(key)) {
        klist = PyObject_CallMethod(key, "split", "s", sep);
        k = from_python(self->ctx, klist);
        Py_XDECREF(klist);
    } else {
        k = from_python(self->ctx, key);
    }

    if (!PyErr_Occurred()) {
        if (k->type == List) {
            r = db_multi(SELF_CTX_AND_DBCOLL, k, multi_GET, &v, 0);
            if (r == 0) {
                ret = to_python(self->ctx, v, 1);
            } else if (dflt) {
                Py_INCREF(dflt);
                ret = dflt;
            } else {
                PyErr_SetObject(PyExc_KeyError, key);
            }
        } else if (dbcollection_getitem(SELF_CTX_AND_DBCOLL, k, &v) == 0) {
            ret = to_python(self->ctx, v, 1);
        } else {
            if (dflt) {
                ret = dflt; Py_INCREF(ret);
            } else {
                PyErr_SetObject(PyExc_KeyError, key);
            }
        }
    }
    dbunlock(self->ctx);
    return ret;
}


static PyObject *
PongoCollection_set(PongoCollection *self, PyObject *args, PyObject *kwargs)
{
    PyObject *key, *value;
    PyObject *klist = NULL;
    PyObject *ret = NULL;
    dbtype_t *k=NULL, *v;
    int sync = self->ctx->sync;
    int fail = 0;
    multi_t op = multi_SET;
    char *kwlist[] = {"key", "value", "sep", "sync", "fail", NULL};
    char *sep = ".";

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|sii:set", kwlist,
                &key, &value, &sep, &sync, &fail))
        return NULL;

    dblock(self->ctx);
    if (PyString_Check(key) || PyUnicode_Check(key)) {
        klist = PyObject_CallMethod(key, "split", "s", sep);
        k = from_python(self->ctx, klist);
        Py_XDECREF(klist);
    } else {
        if (key == pongo_id) {
            sync |= PUT_ID;
        } else {
            k = from_python(self->ctx, key);
        }
    }
    v = from_python(self->ctx, value);
    if (!PyErr_Occurred()) {
        if (k && k->type == List) {
            if (fail) op = multi_SET_OR_FAIL;
            if (db_multi(SELF_CTX_AND_DBCOLL, k, op, &v, sync) == 0) {
                ret = Py_None;
            } else {
                PyErr_SetObject(PyExc_KeyError, key);
            }
        } else if (db_multi(SELF_CTX_AND_DBCOLL, k, op, &v, sync) == 0) {
            // db_mutli will tell us the newly created value of
            // "_id" when PUT_ID is enabled.
            ret = (sync & PUT_ID) ? to_python(self->ctx, v, 1) : Py_None;
        } else {
            if (sync & PUT_ID) {
                PyErr_Format(PyExc_ValueError, "value must be a dictionary");
            } else {
                PyErr_SetObject(PyExc_KeyError, key);
            }
        }
    }
    dbunlock(self->ctx);
    Py_XINCREF(ret);
    return ret;
}

static PyObject *
PongoCollection_pop(PongoCollection *self, PyObject *args, PyObject *kwargs)
{
    PyObject *key, *dflt = NULL;
    PyObject *ret = NULL;
    dbtype_t *k, *v;
    int sync = self->ctx->sync;
    char *kwlist[] = {"key", "default", "sync", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|Oi:pop", kwlist,
                &key, &dflt, &sync))
        return NULL;

    dblock(self->ctx);
    k = from_python(self->ctx, key);
    if (!PyErr_Occurred()) {
        if (dbcollection_delitem(SELF_CTX_AND_DBCOLL, k, &v, sync) < 0) {
            if (dflt) {
                Py_INCREF(dflt);
                ret = dflt;
            } else {
                PyErr_SetObject(PyExc_KeyError, key);
            }
        } else {
            ret = to_python(self->ctx, v, 1);
        }
    }
    dbunlock(self->ctx);
    return ret;
}

typedef struct {
    int type;
    PyObject *ob;
} kvi_t;

static void
kvi_helper(pgctx_t *ctx, dbtype_t *node, void *user)
{
    kvi_t *kvi = (kvi_t*)user;
    PyObject *k, *v, *item;

    if (kvi->type == 0) {
        item = to_python(ctx, _ptr(ctx, node->key), 1);
        PyList_Append(kvi->ob, item);
        Py_DECREF(item);
    } else if (kvi->type == 1) {
        item = to_python(ctx, _ptr(ctx, node->value), 1);
        PyList_Append(kvi->ob, item);
        Py_DECREF(item);
    } else if (kvi->type == 2) {
        k = to_python(ctx, _ptr(ctx, node->key), 1);
        v = to_python(ctx, _ptr(ctx, node->value), 1);
        item = Py_BuildValue("(OO)", k, v);
        PyList_Append(kvi->ob, item);
        Py_DECREF(k); Py_DECREF(v); Py_DECREF(item);
    } else {
    }
}


static PyObject *
PongoCollection_keys(PongoCollection *self)
{
    kvi_t kvi;
    dbtype_t *obj;

    kvi.type = 0;
    kvi.ob = PyList_New(0);
    dblock(self->ctx);
    obj = _ptr(self->ctx, self->dbcoll);
    obj = _ptr(self->ctx, obj->obj);
    bonsai_foreach(self->ctx, obj, kvi_helper, &kvi);
    dbunlock(self->ctx);
    return kvi.ob;
}

static PyObject *
PongoCollection_values(PongoCollection *self)
{
    kvi_t kvi;
    dbtype_t *obj;

    kvi.type = 1;
    kvi.ob = PyList_New(0);
    dblock(self->ctx);
    obj = _ptr(self->ctx, self->dbcoll);
    obj = _ptr(self->ctx, obj->obj);
    bonsai_foreach(self->ctx, obj, kvi_helper, &kvi);
    dbunlock(self->ctx);
    return kvi.ob;
}

static PyObject *
PongoCollection_items(PongoCollection *self)
{
    kvi_t kvi;
    dbtype_t *obj;

    kvi.type = 2;
    kvi.ob = PyList_New(0);
    dblock(self->ctx);
    obj = _ptr(self->ctx, self->dbcoll);
    obj = _ptr(self->ctx, obj->obj);
    bonsai_foreach(self->ctx, obj, kvi_helper, &kvi);
    dbunlock(self->ctx);
    return kvi.ob;
}

static PyObject *
PongoCollection_native(PongoCollection *self)
{
    PyObject *ret;
    dblock(self->ctx);
    ret = to_python(SELF_CTX_AND_DBCOLL, 0);
    dbunlock(self->ctx);
    return ret;
}

static PyObject *
PongoCollection_json(PongoCollection *self, PyObject *args)
{
    char *key = NULL, *val = NULL;
    Py_ssize_t klen, vlen;
    PyObject *ret = Py_None;
    dbtype_t *dict, *obj, *k;
    jsonctx_t *jctx;

    if (!PyArg_ParseTuple(args, "|s#s#:json", &key, &klen, &val, &vlen))
        return NULL;

    dblock(self->ctx);
    dict = _ptr(self->ctx, self->dbcoll);
    jctx = json_init(self->ctx);
    if (key) {
        if (val) {
            // 2-arg form is dict.json('key', 'value')
            // inserts dict['key'] = json_parse('value')
            k = dbstring_new(self->ctx, key, klen);
            obj = json_parse(jctx, val, vlen);
            dbcollection_setitem(SELF_CTX_AND_DBCOLL, k, obj, self->ctx->sync);
            Py_INCREF(ret);
        } else {
            // 1-arg form is replace dict.items with parsed json
            // obj = json_parse(jctx, key, klen);
            // dict->obj = obj->obj;
            PyErr_Format(PyExc_NotImplementedError, "PongoCollection supports only the 0 or 2 arg forms of the json call");
            ret = NULL;
        }
    } else {
        // The 0-arg form is to generate the json string from dictionary
        // contents
        json_emit(jctx, dict);
        if (jctx->outstr)
            ret = PyUnicode_FromStringAndSize(
                (const char*)jctx->outstr, jctx->outlen);
    }
    json_cleanup(jctx);
    dbunlock(self->ctx);
    return ret;
}

static PyObject *
PongoCollection_search(PongoCollection *self, PyObject *args)
{
    PyObject *path, *value, *ret=NULL;
    dbtype_t *dbpath, *dbvalue, *dbrslt;
    char *rel;
    char *sep = ".";
    int decpath = 0;
    relop_t relop;

    if (!PyArg_ParseTuple(args, "OsO:search", &path, &rel, &value))
        return NULL;

    if (!strcmp(rel, "==") || !strcmp(rel, "eq")) {
        relop = db_EQ;
    } else if (!strcmp(rel, "!=") || !strcmp(rel, "ne")) {
        relop = db_NE;
    } else if (!strcmp(rel, "<") || !strcmp(rel, "lt")) {
        relop = db_LT;
    } else if (!strcmp(rel, "<=") || !strcmp(rel, "le")) {
        relop = db_LE;
    } else if (!strcmp(rel, ">") || !strcmp(rel, "gt")) {
        relop = db_GT;
    } else if (!strcmp(rel, ">=") || !strcmp(rel, "ge")) {
        relop = db_GE;
    } else {
        PyErr_Format(PyExc_ValueError, "Unknown relop '%s'", rel);
        return NULL;
    }

    if (PyString_Check(path)) {
        path = PyObject_CallMethod(path, "split", "s", sep);
        decpath = 1;
    }

    if (!PySequence_Check(path)) {
        PyErr_Format(PyExc_TypeError, "path must be a sequence");
        return NULL;
    }
    dblock(self->ctx);
    dbpath = from_python(self->ctx, path);
    if (decpath)
        Py_DECREF(path);
    if (dbpath->type == List) {
        dbvalue = from_python(self->ctx, value);
        if (!PyErr_Occurred()) {
            dbrslt = dbcollection_new(self->ctx);
            db_search(SELF_CTX_AND_DBCOLL, dbpath, -1, relop, dbvalue, dbrslt);
            // proxy=-1 means turn the root object into a real dict, but
            // create proxy objects for all children.
            ret = to_python(self->ctx, dbrslt, -1);
        }
    } else {
        PyErr_Format(PyExc_Exception, "path type isn't List (%d)", dbpath->type);
    }
    dbunlock(self->ctx);
    return ret;
}

static PyObject *
PongoCollection_create(PyObject *self, PyObject *arg)
{
    PyObject *ret;
    PongoCollection *ref = (PongoCollection*)arg;
    dbtype_t *coll;

    if (pongo_check(ref))
        return NULL;

    dblock(ref->ctx);
    coll = dbcollection_new(ref->ctx);
    ret = to_python(ref->ctx, coll, 1);
    dbunlock(ref->ctx);
    return ret;
}

static PyObject *
PongoCollection_repr(PyObject *ob)
{
    PongoCollection *self = (PongoCollection*)ob;
    char buf[32];
    sprintf(buf, "0x%" PRIx64, self->dbcoll);
    return PyString_FromFormat("PongoCollection(%p, %s)", self->ctx, buf);
}

void PongoCollection_Del(PyObject *ob)
{
    PongoCollection *self = (PongoCollection*)ob;
    dblock(self->ctx);
    pidcache_del(self->ctx, self);
    dbunlock(self->ctx);
    PyObject_Del(ob);
}

static PyMappingMethods pydbdict_as_mapping = {
    (lenfunc)PongoCollection_length,           /* mp_length */
    (binaryfunc)PongoCollection_GetItem,       /* mp_subscript */
    (objobjargproc)PongoCollection_SetItem,    /* mp_ass_subscript */
};

static PySequenceMethods pydbdict_as_sequence = {
    (lenfunc)PongoCollection_length,           /* sq_length */
    NULL,                               /* sq_concat */
    NULL,                               /* sq_repeat */
    NULL,                               /* sq_item */
    NULL,                               /* sq_slice */
    NULL,                               /* sq_ass_item */
    NULL,                               /* sq_ass_slice */
    (objobjproc)PongoCollection_contains,      /* sq_contains */
    NULL,                               /* sq_inplace_concat */
    NULL,                               /* sq_inplace_repeat */
};

static PyMethodDef pydbdict_methods[] = {
    {"get",     (PyCFunction)PongoCollection_get,          METH_VARARGS|METH_KEYWORDS, NULL },
    {"set",     (PyCFunction)PongoCollection_set,          METH_VARARGS|METH_KEYWORDS, NULL },
    {"pop",     (PyCFunction)PongoCollection_pop,          METH_VARARGS|METH_KEYWORDS, NULL },
    {"keys",    (PyCFunction)PongoCollection_keys,         METH_NOARGS, NULL },
    {"values",  (PyCFunction)PongoCollection_values,       METH_NOARGS, NULL },
    {"items",   (PyCFunction)PongoCollection_items,        METH_NOARGS, NULL },
    {"native",  (PyCFunction)PongoCollection_native,       METH_NOARGS, NULL },
    {"create",  (PyCFunction)PongoCollection_create,       METH_STATIC|METH_O, NULL },
    {"json",    (PyCFunction)PongoCollection_json,         METH_VARARGS, NULL },
    {"search",  (PyCFunction)PongoCollection_search,       METH_VARARGS, NULL },
    { NULL, NULL },
};

PyTypeObject PongoCollection_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "_pongo.PongoCollection",         /*tp_name*/
    sizeof(PongoCollection), /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)PongoCollection_Del,  /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    PongoCollection_repr,             /*tp_repr*/
    0,                         /*tp_as_number*/
    &pydbdict_as_sequence,     /*tp_as_sequence*/
    &pydbdict_as_mapping,      /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,        /*tp_flags*/
    "PongoDB Collection Proxy",           /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    pydbdict_methods,          /* tp_methods */
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
