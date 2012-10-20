#include "pongo.h"

/*************************************************************************
 * PongoDict Proxy implementation
 *
 * Present a python dict-like interface to the underlying dbobj type.
 ************************************************************************/

PyObject*
PongoDict_Proxy(pgctx_t *ctx, dbtype_t db)
{
    PongoDict *self;

    self = (PongoDict *)PyObject_New(PongoDict, &PongoDict_Type);
    if (!self)
        return NULL;

    self->ctx = ctx;
    self->dbobj = db;
    return (PyObject *)self;
}

static PyObject *
PongoDict_GetItem(PongoDict *self, PyObject *key)
{
    dbtype_t k, v;
    PyObject *ret = NULL;

    dblock(self->ctx);
    k = from_python(self->ctx, key);
    if (!PyErr_Occurred()) {
        if (dbobject_getitem(SELF_CTX_AND_DBOBJ, k, &v) == 0) {
            ret = to_python(self->ctx, v, 1);
        } else {
            PyErr_SetObject(PyExc_KeyError, key);
        }
    }
    dbunlock(self->ctx);
    return ret;
}

static int
PongoDict_SetItem(PongoDict *self, PyObject *key, PyObject *value)
{
    dbtype_t k;
    dbtype_t v;
    int ret = -1;

    dblock(self->ctx);
    k = from_python(self->ctx, key);
    if (!PyErr_Occurred()) {
        if (value == NULL) {
            if (dbobject_delitem(SELF_CTX_AND_DBOBJ, k, &v, self->ctx->sync) == 0) {
                ret = 0;
            } else {
                PyErr_SetObject(PyExc_KeyError, key);
            }
        } else {
            v = from_python(self->ctx, value);
            if (!PyErr_Occurred() && dbobject_setitem(SELF_CTX_AND_DBOBJ, k, v, self->ctx->sync) == 0)
                ret = 0;
        }
    }
    dbunlock(self->ctx);
    return ret;
}

static int
PongoDict_length(PongoDict *self)
{
    int len;

    dblock(self->ctx);
    len = dbobject_len(SELF_CTX_AND_DBOBJ);
    dbunlock(self->ctx);
    return len;
}

static int
PongoDict_contains(PongoDict *self, PyObject *key)
{
    dbtype_t k;
    int ret = 0;

    dblock(self->ctx);
    k = from_python(self->ctx, key);
    if (!PyErr_Occurred()) {
        ret = dbobject_contains(SELF_CTX_AND_DBOBJ, k);
    }
    dbunlock(self->ctx);
    return ret;
}

static PyObject *
PongoDict_get(PongoDict *self, PyObject *args, PyObject *kwargs)
{
    PyObject *key, *dflt = Py_None;
    PyObject *klist = NULL;
    PyObject *ret = NULL;
    dbtype_t k, v;
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
        if (dbtype(self->ctx, k) == List) {
            r = db_multi(SELF_CTX_AND_DBOBJ, k, multi_GET, &v, 0);
            if (r == 0) {
                ret = to_python(self->ctx, v, 1);
            } else if (dflt) {
                Py_INCREF(dflt);
                ret = dflt;
            } else {
                PyErr_SetObject(PyExc_KeyError, key);
            }
        } else if (dbobject_getitem(SELF_CTX_AND_DBOBJ, k, &v) == 0) {
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
PongoDict_set(PongoDict *self, PyObject *args, PyObject *kwargs)
{
    PyObject *key, *value;
    PyObject *klist = NULL;
    PyObject *ret = NULL;
    dbtype_t k, v;
    int sync = self->ctx->sync;
    int fail = 0;
    multi_t op = multi_SET;
    char *kwlist[] = {"key", "value", "sep", "sync", "fail", NULL};
    char *sep = ".";

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|sii:set", kwlist,
                &key, &value, &sep, &sync, &fail))
        return NULL;

    k = DBNULL;
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
        if (dbtype(self->ctx, k) == List) {
            if (fail) op = multi_SET_OR_FAIL;
            if (db_multi(SELF_CTX_AND_DBOBJ, k, op, &v, sync) == 0) {
                ret = Py_None;
            } else {
                PyErr_SetObject(PyExc_KeyError, key);
            }
        } else if (db_multi(SELF_CTX_AND_DBOBJ, k, op, &v, sync) == 0) {
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
PongoDict_pop(PongoDict *self, PyObject *args, PyObject *kwargs)
{
    PyObject *key, *dflt = NULL;
    PyObject *ret = NULL;
    dbtype_t k, v;
    int sync = self->ctx->sync;
    char *kwlist[] = {"key", "default", "sync", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|Oi:pop", kwlist,
                &key, &dflt, &sync))
        return NULL;

    dblock(self->ctx);
    k = from_python(self->ctx, key);
    if (!PyErr_Occurred()) {
        if (dbobject_delitem(SELF_CTX_AND_DBOBJ, k, &v, sync) < 0) {
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


static PyObject *
PongoDict_keys(PongoDict *self) {
    dbtype_t db;
    _obj_t *obj;
    PyObject *ret = PyList_New(0);
    PyObject *item;
    int i;

    dblock(self->ctx);
    db.ptr = dbptr(self->ctx, self->dbobj);
    obj = dbptr(self->ctx, db.ptr->obj);
    for(i=0; i<obj->len; i++) {
        // FIXME: NULL is a valid key?
        if (obj->item[i].key.all) {
            item = to_python(self->ctx, obj->item[i].key, 1);
            PyList_Append(ret, item);
            Py_DECREF(item);
        }
    }
    dbunlock(self->ctx);
    return ret;
}

static PyObject *
PongoDict_values(PongoDict *self)
{
    dbtype_t db;
    _obj_t *obj;
    PyObject *ret = PyList_New(0);
    PyObject *item;
    int i;

    dblock(self->ctx);
    db.ptr = dbptr(self->ctx, self->dbobj);
    obj = dbptr(self->ctx, db.ptr->obj);
    for(i=0; i<obj->len; i++) {
        // FIXME: NULL is a valid key?
        if (obj->item[i].key.all) {
            item = to_python(self->ctx, obj->item[i].value, 1);
            PyList_Append(ret, item);
            Py_DECREF(item);
        }
    }
    dbunlock(self->ctx);
    return ret;
}

static PyObject *
PongoDict_items(PongoDict *self) {
    dbtype_t db;
    _obj_t *obj;
    PyObject *ret = PyList_New(0);
    PyObject *item, *k, *v;
    int i;

    dblock(self->ctx);
    db.ptr = dbptr(self->ctx, self->dbobj);
    obj = dbptr(self->ctx, db.ptr->obj);
    for(i=0; i<obj->len; i++) {
        // FIXME: NULL is a valid key?
        if (obj->item[i].key.all) {
            k = to_python(self->ctx, obj->item[i].key, 1);
            v = to_python(self->ctx, obj->item[i].value, 1);
            item = Py_BuildValue("(OO)", k, v);
            PyList_Append(ret, item);
            Py_DECREF(k); Py_DECREF(v); Py_DECREF(item);
        }
    }
    dbunlock(self->ctx);
    return ret;
}

static PyObject *
PongoDict_native(PongoDict *self)
{
    PyObject *ret;
    dblock(self->ctx);
    ret = to_python(SELF_CTX_AND_DBOBJ, 0);
    dbunlock(self->ctx);
    return ret;
}

static PyObject *
PongoDict_json(PongoDict *self, PyObject *args)
{
    char *key = NULL, *val = NULL;
    Py_ssize_t klen, vlen;
    PyObject *ret = Py_None;
    dbtype_t dict, obj, k;
    jsonctx_t *jctx;

    if (!PyArg_ParseTuple(args, "|s#s#:json", &key, &klen, &val, &vlen))
        return NULL;

    dblock(self->ctx);
    dict = self->dbobj;
    jctx = json_init(self->ctx);
    if (key) {
        if (val) {
            // 2-arg form is dict.json('key', 'value')
            // inserts dict['key'] = json_parse('value')
            k = dbstring_new(self->ctx, key, klen);
            obj = json_parse(jctx, val, vlen);
            dbobject_setitem(SELF_CTX_AND_DBOBJ, k, obj, self->ctx->sync);
        } else {
            // 1-arg form is replace dict.items with parsed json
            obj = json_parse(jctx, key, klen);
            dict.ptr = dbptr(self->ctx, dict);
            obj.ptr = dbptr(self->ctx, obj);
            dict.ptr->obj = obj.ptr->obj;
        }
        Py_INCREF(ret);
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
PongoDict_search(PongoDict *self, PyObject *args)
{
    PyObject *path, *value, *ret=NULL;
    dbtype_t dbpath, dbvalue, dbrslt;
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
    if (dbtype(self->ctx, dbpath) == List) {
        dbvalue = from_python(self->ctx, value);
        if (!PyErr_Occurred()) {
            dbrslt = dbcollection_new(self->ctx);
            db_search(SELF_CTX_AND_DBOBJ, dbpath, -1, relop, dbvalue, dbrslt);
            // proxy=-1 means turn the root object into a real dict, but
            // create proxy objects for all children.
            ret = to_python(self->ctx, dbrslt, -1);
        }
    } else {
        PyErr_Format(PyExc_Exception, "path type isn't List (%d)", dbtype(self->ctx, dbpath));
    }
    dbunlock(self->ctx);
    return ret;
}

static PyObject *
PongoDict_create(PyObject *self, PyObject *arg)
{
    PyObject *ret;
    PongoCollection *ref = (PongoCollection*)arg;
    dbtype_t dict;

    if (pongo_check(ref))
        return NULL;

    dblock(ref->ctx);
    dict = dbobject_new(ref->ctx);
    ret = to_python(ref->ctx, dict, 1);
    dbunlock(ref->ctx);
    return ret;
}

static PyObject *
PongoDict_repr(PyObject *ob)
{
    PongoDict *self = (PongoDict*)ob;
    char buf[32];
    sprintf(buf, "0x%" PRIx64, self->dbobj.all);
    return PyString_FromFormat("PongoDict(%p, %s)", self->ctx, buf);
}

void PongoDict_Del(PyObject *ob)
{
    PongoDict *self = (PongoDict*)ob;
    dblock(self->ctx);
    pidcache_del(self->ctx, self);
    dbunlock(self->ctx);
    PyObject_Del(ob);
}

static PyMappingMethods pydbdict_as_mapping = {
    (lenfunc)PongoDict_length,           /* mp_length */
    (binaryfunc)PongoDict_GetItem,       /* mp_subscript */
    (objobjargproc)PongoDict_SetItem,    /* mp_ass_subscript */
};

static PySequenceMethods pydbdict_as_sequence = {
    (lenfunc)PongoDict_length,           /* sq_length */
    NULL,                               /* sq_concat */
    NULL,                               /* sq_repeat */
    NULL,                               /* sq_item */
    NULL,                               /* sq_slice */
    NULL,                               /* sq_ass_item */
    NULL,                               /* sq_ass_slice */
    (objobjproc)PongoDict_contains,      /* sq_contains */
    NULL,                               /* sq_inplace_concat */
    NULL,                               /* sq_inplace_repeat */
};

static PyMethodDef pydbdict_methods[] = {
    {"get",     (PyCFunction)PongoDict_get,          METH_VARARGS|METH_KEYWORDS, NULL },
    {"set",     (PyCFunction)PongoDict_set,          METH_VARARGS|METH_KEYWORDS, NULL },
    {"pop",     (PyCFunction)PongoDict_pop,          METH_VARARGS|METH_KEYWORDS, NULL },
    {"keys",    (PyCFunction)PongoDict_keys,         METH_NOARGS, NULL },
    {"values",  (PyCFunction)PongoDict_values,       METH_NOARGS, NULL },
    {"items",   (PyCFunction)PongoDict_items,        METH_NOARGS, NULL },
    {"native",  (PyCFunction)PongoDict_native,       METH_NOARGS, NULL },
    {"create",  (PyCFunction)PongoDict_create,       METH_STATIC|METH_O, NULL },
    {"json",    (PyCFunction)PongoDict_json,         METH_VARARGS, NULL },
    {"search",  (PyCFunction)PongoDict_search,       METH_VARARGS, NULL },
    { NULL, NULL },
};

PyTypeObject PongoDict_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "_pongo.PongoDict",         /*tp_name*/
    sizeof(PongoDict), /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)PongoDict_Del,  /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    PongoDict_repr,             /*tp_repr*/
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
    "PongoDB Dict Proxy",           /* tp_doc */
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
