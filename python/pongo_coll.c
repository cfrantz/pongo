#include "pongo.h"
#include <pongo/bonsai.h>

/*************************************************************************
 * PongoCollection Proxy implementation
 *
 * Present a python dict-like interface to the underlying dbcollection type.
 ************************************************************************/

PyObject*
PongoCollection_Proxy(pgctx_t *ctx, dbtype_t db)
{
    PongoCollection *self;

    self = (PongoCollection *)PyObject_New(PongoCollection, &PongoCollection_Type);
    if (!self)
        return NULL;

    self->ctx = ctx;
    self->dbptr = db;
    return (PyObject *)self;
}

static PyObject *
PongoCollection_GetItem(PongoCollection *self, PyObject *key)
{
    dbtype_t k, v;
    PyObject *ret = NULL;

    dblock(self->ctx);
    k = from_python(self->ctx, key);
    if (!PyErr_Occurred()) {
        if (dbcollection_getnode(SELF_CTX_AND_DBPTR, k, &v) == 0) {
            ret = to_python(self->ctx, v, TP_PROXY | TP_NODEVAL);
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
    dbtype_t k;
    dbtype_t v = DBNULL;
    int ret = -1;

    dblock(self->ctx);
    if (PyTuple_Check(key)) {
        if (PyTuple_Size(key) == 2) {
            k = from_python(self->ctx, PyTuple_GetItem(key, 0));
            v = from_python(self->ctx, PyTuple_GetItem(key, 1));
        } else {
            PyErr_Format(PyExc_ValueError, "key tuple must be a 2-tuple");
        }
    } else {
        k = from_python(self->ctx, key);
    }

    if (!PyErr_Occurred()) {
        if (value == NULL) {
            if (dbcollection_delitem(SELF_CTX_AND_DBPTR, k, &v, self->ctx->sync) == 0) {
                ret = 0;
            } else {
                PyErr_SetObject(PyExc_KeyError, key);
            }
        } else {
            v = from_python(self->ctx, value);
            if (!PyErr_Occurred() && dbcollection_setitem(SELF_CTX_AND_DBPTR, k, v, self->ctx->sync) == 0)
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
    len = dbcollection_len(SELF_CTX_AND_DBPTR);
    dbunlock(self->ctx);
    return len;
}

static int
PongoCollection_contains(PongoCollection *self, PyObject *key)
{
    dbtype_t k;
    int ret = 0;

    dblock(self->ctx);
    k = from_python(self->ctx, key);
    if (!PyErr_Occurred()) {
        ret = dbcollection_contains(SELF_CTX_AND_DBPTR, k);
    }
    dbunlock(self->ctx);
    return ret;
}

PyDoc_STRVAR(get_doc,
"C.get(key, [default, [sep]]) -> item -- Get C[key] if it exists, else default.\n"
"key may be a string or list of strings.  Get will traverse into child objects\n"
"to retrieve the requested object");
static PyObject *
PongoCollection_get(PongoCollection *self, PyObject *args, PyObject *kwargs)
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
            r = db_multi(SELF_CTX_AND_DBPTR, k, multi_GET, &v, 0);
            if (r == 0) {
                ret = to_python(self->ctx, v, TP_PROXY);
            } else if (dflt) {
                Py_INCREF(dflt);
                ret = dflt;
            } else {
                PyErr_SetObject(PyExc_KeyError, key);
            }
        } else if (dbcollection_getnode(SELF_CTX_AND_DBPTR, k, &v) == 0) {
            ret = to_python(self->ctx, v, TP_PROXY | TP_NODEVAL);
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

PyDoc_STRVAR(set_doc,
"C.set(key, value, [sep, [sync, [fail]]]) -- Insert or modify C[key].\n"
"Key maybe a string or list of strings.  Set will traverse into child\n"
"objects to set the value.\n"
"\n"
"sync determines whether or not to do a syncrhonize after the set.\n"
"fail makes the set operation into 'set or fail'.  Trying to set an\n"
"key that already exits will result in KeyError");
static PyObject *
PongoCollection_set(PongoCollection *self, PyObject *args, PyObject *kwargs)
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
            if (db_multi(SELF_CTX_AND_DBPTR, k, op, &v, sync) == 0) {
                ret = Py_None;
            } else {
                PyErr_SetObject(PyExc_KeyError, key);
            }
        } else if (db_multi(SELF_CTX_AND_DBPTR, k, op, &v, sync) == 0) {
            // db_mutli will tell us the newly created value of
            // "_id" when PUT_ID is enabled.
            ret = (sync & PUT_ID) ? to_python(self->ctx, v, TP_PROXY) : Py_None;
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

PyDoc_STRVAR(pop_doc,
"C.pop(key, [default, [sync]]) -> item -- Get and remove C[key] if it exists.\n");
static PyObject *
PongoCollection_pop(PongoCollection *self, PyObject *args, PyObject *kwargs)
{
    PyObject *key, *dflt = NULL;
    PyObject *ret = NULL;
    dbtype_t k, v = DBNULL;
    int sync = self->ctx->sync;
    char *kwlist[] = {"key", "default", "sync", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|Oi:pop", kwlist,
                &key, &dflt, &sync))
        return NULL;

    dblock(self->ctx);
    if (PyTuple_Check(key)) {
        if (PyTuple_Size(key) == 2) {
            k = from_python(self->ctx, PyTuple_GetItem(key, 0));
            v = from_python(self->ctx, PyTuple_GetItem(key, 1));
        } else {
            PyErr_Format(PyExc_ValueError, "key tuple must be a 2-tuple");
        }
    } else {
        k = from_python(self->ctx, key);
    }

    if (!PyErr_Occurred()) {
        if (dbcollection_delitem(SELF_CTX_AND_DBPTR, k, &v, sync) < 0) {
            if (dflt) {
                Py_INCREF(dflt);
                ret = dflt;
            } else {
                PyErr_SetObject(PyExc_KeyError, key);
            }
        } else {
            ret = to_python(self->ctx, v, TP_PROXY);
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
kvi_helper(pgctx_t *ctx, dbtype_t node, void *user)
{
    kvi_t *kvi = (kvi_t*)user;
    PyObject *item;

    if (kvi->type == 0) {
        item = to_python(ctx, node, TP_PROXY | TP_NODEKEY);
        PyList_Append(kvi->ob, item);
        Py_DECREF(item);
    } else if (kvi->type == 1) {
        item = to_python(ctx, node, TP_PROXY | TP_NODEVAL);
        PyList_Append(kvi->ob, item);
        Py_DECREF(item);
    } else if (kvi->type == 2) {
        item = to_python(ctx, node, TP_PROXY | TP_NODEKEY | TP_NODEVAL);
        PyList_Append(kvi->ob, item);
        Py_DECREF(item);
    } else {
    }
}


PyDoc_STRVAR(keys_doc,
"C.keys() -> [key, ...] -- Get the list of keys in the collection.");
static PyObject *
PongoCollection_keys(PongoCollection *self)
{
    kvi_t kvi;
    dbtype_t obj;

    kvi.type = 0;
    kvi.ob = PyList_New(0);
    dblock(self->ctx);
    obj.ptr = dbptr(self->ctx, self->dbptr);
    bonsai_foreach(self->ctx, obj.ptr->obj, kvi_helper, &kvi);
    dbunlock(self->ctx);
    return kvi.ob;
}

PyDoc_STRVAR(values_doc,
"C.values() -> [value, ...] -- Get the list of values in the collection.");
static PyObject *
PongoCollection_values(PongoCollection *self)
{
    kvi_t kvi;
    dbtype_t obj;

    kvi.type = 1;
    kvi.ob = PyList_New(0);
    dblock(self->ctx);
    obj.ptr = dbptr(self->ctx, self->dbptr);
    bonsai_foreach(self->ctx, obj.ptr->obj, kvi_helper, &kvi);
    dbunlock(self->ctx);
    return kvi.ob;
}

PyDoc_STRVAR(items_doc,
"C.items() -> [(k,v), ...] -- Get the list of items in the collection.");
static PyObject *
PongoCollection_items(PongoCollection *self)
{
    kvi_t kvi;
    dbtype_t obj;

    kvi.type = 2;
    kvi.ob = PyList_New(0);
    dblock(self->ctx);
    obj.ptr = dbptr(self->ctx, self->dbptr);
    bonsai_foreach(self->ctx, obj.ptr->obj, kvi_helper, &kvi);
    dbunlock(self->ctx);
    return kvi.ob;
}

PyDoc_STRVAR(native_doc,
"C.native() -> {...} -- Return a native python dict with the same items as C.");
static PyObject *
PongoCollection_native(PongoCollection *self)
{
    PyObject *ret;
    dblock(self->ctx);
    ret = to_python(SELF_CTX_AND_DBPTR, 0);
    dbunlock(self->ctx);
    return ret;
}

PyDoc_STRVAR(multi_doc,
"C.multi() -> bool -- Return whether C is a key-multival collection");
static PyObject *
PongoCollection_multi(PongoCollection *self)
{
    PyObject *ret;
    dbtype_t obj;

    dblock(self->ctx);
    obj.ptr = dbptr(self->ctx, self->dbptr);
    ret = obj.ptr->type == MultiCollection ? Py_True : Py_False;
    dbunlock(self->ctx);
    Py_INCREF(ret);
    return ret;
}

PyDoc_STRVAR(json_doc,
"C.json([key, value]) -- Return or parse JSON string\n"
"C.json() -> str -- Return a JSON encoded string representing C.\n"
"C.json(key, value) -- Parse value as a JSON string and assign it to C[key].\n");
static PyObject *
PongoCollection_json(PongoCollection *self, PyObject *args)
{
    char *key = NULL, *val = NULL;
    Py_ssize_t klen, vlen;
    PyObject *ret = Py_None;
    dbtype_t dict, obj, k;
    jsonctx_t *jctx;

    if (!PyArg_ParseTuple(args, "|s#s#:json", &key, &klen, &val, &vlen))
        return NULL;

    dblock(self->ctx);
    dict = self->dbptr;
    jctx = json_init(self->ctx);
    if (key) {
        if (val) {
            // 2-arg form is dict.json('key', 'value')
            // inserts dict['key'] = json_parse('value')
            k = dbstring_new(self->ctx, key, klen);
            obj = json_parse(jctx, val, vlen);
            dbcollection_setitem(SELF_CTX_AND_DBPTR, k, obj, self->ctx->sync);
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

PyDoc_STRVAR(search_doc,
"C.search(key, relop, value) -> Collection -- Find all children of C who\n"
"have child[key] relop value.\n");
static PyObject *
PongoCollection_search(PongoCollection *self, PyObject *args)
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
            dbrslt = dbcollection_new(self->ctx, 0);
            db_search(SELF_CTX_AND_DBPTR, dbpath, -1, relop, dbvalue, dbrslt);
            // PROXYCHLD means turn the root object into a real dict, but
            // create proxy objects for all children.
            ret = to_python(self->ctx, dbrslt, TP_PROXYCHLD);
        }
    } else {
        PyErr_Format(PyExc_Exception, "path type isn't List (%d)", dbtype(self->ctx, dbpath));
    }
    dbunlock(self->ctx);
    return ret;
}

PyDoc_STRVAR(create_doc,
"PongoCollection.create([multi]) -- Create a new collection.\n"
"multi determines whether the new collection is key-value or key-multivalue.");
static PyObject *
PongoCollection_create(PyObject *self, PyObject *args)
{
    PyObject *ret;
    PongoCollection *ref = NULL;
    dbtype_t coll;
    int multi = 0;

    if (!PyArg_ParseTuple(args, "O|i:create", &ref, &multi))
        return NULL;
    if (pongo_check(ref))
        return NULL;

    dblock(ref->ctx);
    coll = dbcollection_new(ref->ctx, multi);
    ret = to_python(ref->ctx, coll, TP_PROXY);
    dbunlock(ref->ctx);
    return ret;
}

static PyObject *
PongoCollection_repr(PyObject *ob)
{
    PongoCollection *self = (PongoCollection*)ob;
    char buf[32];
    sprintf(buf, "0x%" PRIx64, self->dbptr.all);
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

static PyObject *
PongoCollection_getattr(PyObject *ob, PyObject *name)
{
    PongoCollection *self = (PongoCollection*)ob;
    dbtype_t coll;
    char *buf;

    buf = PyString_AsString(name);
    if (buf && !strcmp(buf, "index")) {
        dblock(self->ctx);
        coll.ptr = dbptr(self->ctx, self->dbptr);
        if (self->index.all != coll.ptr->index.all) {
            self->index = coll.ptr->index;
            self->index_ob = to_python(self->ctx, self->index, TP_PROXY);
        }
        dbunlock(self->ctx);
    }
    return PyObject_GenericGetAttr(ob, name);
}

static int
PongoCollection_setattr(PyObject *ob, PyObject *name, PyObject *value)
{
    PongoCollection *self = (PongoCollection*)ob;
    dbtype_t coll;
    char *buf;

    buf = PyString_AsString(name);
    if (buf && !strcmp(buf, "index")) {
        dblock(self->ctx);
        coll.ptr = dbptr(self->ctx, self->dbptr);
        if (value) {
            coll.ptr->index = from_python(self->ctx, value);
        } else {
            coll.ptr->index = DBNULL;
        }

        // We can just zap these out, since these aren't used until
        // the user gets them via getattr.
        self->index = DBNULL;
        Py_XDECREF(self->index_ob);
        self->index_ob = NULL;
        dbunlock(self->ctx);
        return 0;
    }
    return PyObject_GenericSetAttr(ob, name, value);
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

static PyMethodDef pycoll_methods[] = {
    {"get",     (PyCFunction)PongoCollection_get,          METH_VARARGS|METH_KEYWORDS, get_doc },
    {"set",     (PyCFunction)PongoCollection_set,          METH_VARARGS|METH_KEYWORDS, set_doc },
    {"pop",     (PyCFunction)PongoCollection_pop,          METH_VARARGS|METH_KEYWORDS, pop_doc },
    {"keys",    (PyCFunction)PongoCollection_keys,         METH_NOARGS, keys_doc },
    {"values",  (PyCFunction)PongoCollection_values,       METH_NOARGS, values_doc },
    {"items",   (PyCFunction)PongoCollection_items,        METH_NOARGS, items_doc },
    {"native",  (PyCFunction)PongoCollection_native,       METH_NOARGS, native_doc },
    {"multi",   (PyCFunction)PongoCollection_multi,        METH_NOARGS, multi_doc },
    {"create",  (PyCFunction)PongoCollection_create,       METH_STATIC|METH_VARARGS, create_doc },
    {"json",    (PyCFunction)PongoCollection_json,         METH_VARARGS, json_doc },
    {"search",  (PyCFunction)PongoCollection_search,       METH_VARARGS, search_doc },
    { NULL, NULL },
};

static PyMemberDef pycoll_members[] = {
    {"index", T_OBJECT_EX, offsetof(PongoCollection, index_ob), 0, NULL },
    { 0 },
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
    PongoCollection_getattr,                         /*tp_getattro*/
    PongoCollection_setattr,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,        /*tp_flags*/
    "PongoDB Collection Proxy",           /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    PongoIter_Iter,                         /* tp_iter */
    0,                         /* tp_iternext */
    pycoll_methods,          /* tp_methods */
    pycoll_members,            /* tp_members */
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
