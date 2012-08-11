#include <Python.h>
#include <datetime.h>
#include <pongo/log.h>
#include <pongo/dbmem.h>
#include <pongo/dbtypes.h>
#include <pongo/json.h>

typedef struct {
    PyObject_HEAD
    pgctx_t *ctx;
    uint64_t dblist;
} PongoList;

typedef struct {
    PyObject_HEAD
    pgctx_t *ctx;
    uint64_t dbobj;
} PongoDict;

#define SELF_CTX_AND_DBLIST self->ctx, _ptr(self->ctx, self->dblist)
#define SELF_CTX_AND_DBOBJ self->ctx, _ptr(self->ctx, self->dbobj)

// Forward declarations
static PyTypeObject PongoList_Type;
static PyTypeObject PongoDict_Type;

static PyObject* PongoList_Proxy(pgctx_t *ctx, dbtype_t *db);
static PyObject* PongoDict_Proxy(pgctx_t *ctx, dbtype_t *db);

static dbtype_t *from_python(pgctx_t *ctx, PyObject *ob);

static PyObject *to_python(pgctx_t *ctx, dbtype_t *db, int proxy)
{
    PyObject *ob = NULL;
    PyObject *k, *v;
    _list_t *list;
    _obj_t *obj;
    _cache_t *cache;
    struct tm tm;
    time_t time;
    long usec;
    int i;
    int64_t ival;

    if (db == NULL)
        Py_RETURN_NONE;

    switch(db->type) {
        case Boolean:
            ob = db->bval ? Py_True : Py_False;
            Py_INCREF(ob);
            break;
        case Int:
            ival = db->ival;
            if (ival < LONG_MIN || ival > LONG_MAX) {
                ob = PyLong_FromLongLong(ival);
            } else {
                ob = PyInt_FromLong((long)ival);
            }
            break;
        case Float:
            ob = PyFloat_FromDouble(db->fval);
            break;
        case ByteBuffer:
            ob = PyString_FromStringAndSize((const char*)db->sval, db->len);
            break;
        case String:
            ob = PyUnicode_FromStringAndSize((const char*)db->sval, db->len);
            break;
        case Datetime:
            time = db->utctime / 1000000LL;
            usec = db->utctime % 1000000LL;
#ifdef WIN32
            memcpy(&tm, gmtime(&time), sizeof(tm));
#else
            gmtime_r(&time, &tm);
#endif
            ob = PyDateTime_FromDateAndTime(
                    tm.tm_year+1900, tm.tm_mon, tm.tm_mday,
                    tm.tm_hour, tm.tm_min, tm.tm_sec, usec);
            break;
        case List:
            if (proxy==1) {
                ob = PongoList_Proxy(ctx, db);
            } else {
                if (proxy == -1) proxy = 1;
                list = _ptr(ctx, db->list);
                ob = PyList_New(0);
                for(i=0; i<list->len; i++) {
                    v = to_python(ctx, _ptr(ctx, list->item[i]), proxy);
                    PyList_Append(ob, v);
                    Py_DECREF(v);
                }
            }
            break;
        case Object:
            if (proxy==1) {
                ob = PongoDict_Proxy(ctx, db);
            } else {
                if (proxy == -1) proxy = 1;
                obj = _ptr(ctx, db->obj);
                ob = PyDict_New();
                for(i=0; i<obj->len; i++) {
                    if (obj->item[i].key) {
                        k = to_python(ctx, _ptr(ctx, obj->item[i].key), proxy);
                        v = to_python(ctx, _ptr(ctx, obj->item[i].value), proxy);
                        PyDict_SetItem(ob, k, v);
                        Py_DECREF(k); Py_DECREF(v);
                    }
                }
            }
            break;
        case Cache:
            ob = PyDict_New();
            cache = _ptr(ctx, db->cache);
            v = NULL;
            for(i=0; i<cache->len; i++) {
                if (cache->item[i]) {
                    k = PyInt_FromLong(i);
                    v = to_python(ctx, _ptr(ctx, cache->item[i]), 0);
                    PyDict_SetItem(ob, k, v);
                    Py_DECREF(k);
                    Py_DECREF(v);
                }
            }
            break;
        default:
            PyErr_Format(PyExc_Exception, "Cannot handle dbtype %d", db->type);
    }
    return ob;
}

static int sequence_cb(pgctx_t *ctx, int i, dbtype_t **item, void *user)
{
    PyObject *seq = (PyObject*)user;
    *item = from_python(ctx, PySequence_GetItem(seq, i));
    if (PyErr_Occurred()) return -1;
    return 0;
}

static int mapping_cb(pgctx_t *ctx, int i, dbtype_t **key, dbtype_t **value, void *user)
{
    PyObject *map = (PyObject*)user;
    PyObject *item = PySequence_GetItem(map, i);
    *key = from_python(ctx, PySequence_GetItem(item, 0));
    *value = from_python(ctx, PySequence_GetItem(item, 1));
    if (PyErr_Occurred()) return -1;
    return 0;
}

static int itermapping_cb(pgctx_t *ctx, int i, dbtype_t **key, dbtype_t **value, void *user)
{
    PyObject *iter = (PyObject*)user;
    PyObject *item = PyIter_Next(iter);
    *key = from_python(ctx, PySequence_GetItem(item, 0));
    *value = from_python(ctx, PySequence_GetItem(item, 1));
    if (PyErr_Occurred()) return -1;
    return 0;
}

static dbtype_t *from_python(pgctx_t *ctx, PyObject *ob)
{
    dbtype_t *db = NULL;
    char *buf;
    Py_ssize_t length;
    PyObject *items;
    struct tm tm;
    long usec;
    //int i;
    
    if (PyObject_HasAttrString(ob, "__topongo__")) {
        ob = PyObject_CallMethod(ob, "__topongo__", NULL);
        if (PyErr_Occurred())
            return NULL;
    }
    if (ob == Py_None) {
        db = NULL;
    } else if (PyBool_Check(ob)) {
        db = dbboolean_new(ctx, ob == Py_True);
    } else if (PyInt_Check(ob)) {
        db = dbint_new(ctx, PyInt_AsLong(ob));
    } else if (PyLong_Check(ob)) {
        db = dbint_new(ctx, PyLong_AsLongLong(ob));
    } else if (PyFloat_Check(ob)) {
        db = dbfloat_new(ctx, PyFloat_AsDouble(ob));
    } else if (PyString_Check(ob)) {
        PyString_AsStringAndSize(ob, &buf, &length);
        db = dbbuffer_new(ctx, buf, length);
    } else if (PyUnicode_Check(ob)) {
        PyString_AsStringAndSize(ob, &buf, &length);
        db = dbstring_new(ctx, buf, length);
    } else if (PyDateTime_Check(ob)) {
        memset(&tm, 0, sizeof(tm));
        tm.tm_year = PyDateTime_GET_YEAR(ob);
        tm.tm_mon = PyDateTime_GET_MONTH(ob);
        tm.tm_mday = PyDateTime_GET_DAY(ob);
        tm.tm_hour = PyDateTime_DATE_GET_HOUR(ob);
        tm.tm_min = PyDateTime_DATE_GET_MINUTE(ob);
        tm.tm_sec = PyDateTime_DATE_GET_SECOND(ob);
        usec = PyDateTime_DATE_GET_MICROSECOND(ob);
        tm.tm_year -= 1900;
        db = dbtime_newtm(ctx, &tm, usec);
    } else if (PyMapping_Check(ob)) {
        length = PyMapping_Length(ob);
        items = PyMapping_Items(ob);
        if (items) {
            // mapping object implements "items"
            db = dbobject_new(ctx);
            dbobject_update(ctx, db, length, mapping_cb, items, NOSYNC);
        } else {
            // mapping object implements iterator protocol
            PyErr_Clear();
            items = PyObject_GetIter(ob);
            db = dbobject_new(ctx);
            dbobject_update(ctx, db, length, itermapping_cb, items, NOSYNC);
        }
    } else if (PySequence_Check(ob)) {
        length = PySequence_Length(ob);
        db = dblist_new(ctx);
        dblist_extend(ctx, db, length, sequence_cb, ob, NOSYNC);
    } else if (Py_TYPE(ob) == &PongoList_Type) {
        // Resolve proxy types back to their original dbtype
        PongoList *p = (PongoList*)ob;
        db = _ptr(p->ctx, p->dblist);
    } else if (Py_TYPE(ob) == &PongoDict_Type) {
        // Resolve proxy types back to their original dbtype
        PongoDict *p = (PongoDict*)ob;
        db = _ptr(p->ctx, p->dbobj);
    } else {
        // FIXME: Unknown object type
        PyErr_SetObject(PyExc_TypeError, ob);
        db = NULL;
    }
    return db;
}

void pongo_abort(const char *msg)
{
    PyErr_Format(PyExc_Exception, "Abort: %s", msg);
}

/*************************************************************************
 * PongoList Proxy implementation
 *
 * Present a python list-like interface to the underlying dblist type.
 ************************************************************************/

static PyObject*
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
    if (dblist_getitem(SELF_CTX_AND_DBLIST, i, &item) == 0)
        ret = to_python(self->ctx, item, 1);
    dbunlock(self->ctx);

    return ret;
}

static int
PongoList_SetItem(PongoList *self, Py_ssize_t i, PyObject *v)
{
    dbtype_t *item;
    int ret = -1;

    dblock(self->ctx);
    if (v == NULL) {
        ret = dblist_delitem(SELF_CTX_AND_DBLIST, i, &item, SYNC);
    } else {
        item = from_python(self->ctx, v);
        if (!PyErr_Occurred() && dblist_setitem(SELF_CTX_AND_DBLIST, i, item, SYNC) == 0) {
            ret = 0;
        }
    }
    dbunlock(self->ctx);
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
    if (!PyErr_Occurred() && dblist_insert(SELF_CTX_AND_DBLIST, i, item, sync) == 0) {
        ret = Py_None;
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
    if (!PyErr_Occurred() && dblist_remove(SELF_CTX_AND_DBLIST, item, sync) == 0) {
        ret = Py_None; Py_INCREF(ret);
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
        PyErr_SetString(PyExc_Exception, "PongoList corrupted");
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
    dbtype_t *db;
    dbtype_t *item;
    _list_t *list;
    int i;
    int ret = 0;

    dblock(self->ctx);
    item = from_python(self->ctx, elem);
    if (!PyErr_Occurred()) {
        db = _ptr(self->ctx, self->dblist);
        list = _ptr(self->ctx, db->list);
        for(i=0; i<list->len; i++) {
            if (dbeq(_ptr(self->ctx, list->item[i]), item, 1)) {
                ret = 1; break;
            }
        }
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
    printf("PongoList_Del %p %08llx\n", self, self->dblist);
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

static PyTypeObject PongoList_Type = {
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


/*************************************************************************
 * PongoDict Proxy implementation
 *
 * Present a python dict-like interface to the underlying dbobj type.
 ************************************************************************/

static PyObject*
PongoDict_Proxy(pgctx_t *ctx, dbtype_t *db)
{
    PongoDict *self;

    self = (PongoDict *)PyObject_New(PongoDict, &PongoDict_Type);
    if (!self)
        return NULL;

    self->ctx = ctx;
    self->dbobj = _offset(ctx, db);
    return (PyObject *)self;
}

static PyObject *
PongoDict_GetItem(PongoDict *self, PyObject *key)
{
    dbtype_t *k, *v;
    PyObject *ret = NULL;

    dblock(self->ctx);
    k = from_python(self->ctx, key);
    if (!PyErr_Occurred()) {
        if (dbobject_getitem(SELF_CTX_AND_DBOBJ, k, &v) == 0) {
            ret = to_python(self->ctx, v, 1);
        } else {
            dbfree(self->ctx, k);
            PyErr_SetObject(PyExc_KeyError, key);
        }
    }
    dbunlock(self->ctx);
    return ret;
}

static int
PongoDict_SetItem(PongoDict *self, PyObject *key, PyObject *value)
{
    dbtype_t *k;
    dbtype_t *v;
    int ret = -1;

    dblock(self->ctx);
    k = from_python(self->ctx, key);
    if (!PyErr_Occurred()) {
        if (value == NULL) {
            if (dbobject_delitem(SELF_CTX_AND_DBOBJ, k, &v, SYNC) == 0) {
                ret = 0;
            } else {
                PyErr_SetObject(PyExc_KeyError, key);
            }
            dbfree(self->ctx, k);
        } else {
            v = from_python(self->ctx, value);
            if (!PyErr_Occurred() && dbobject_setitem(SELF_CTX_AND_DBOBJ, k, v, SYNC) == 0)
                ret = 0;
        }
    }
    dbunlock(self->ctx);
    return ret;
}

static int
PongoDict_length(PongoDict *self)
{
    dbtype_t *db;
    _obj_t *obj;
    int len, i;

    dblock(self->ctx);
    db = _ptr(self->ctx, self->dbobj);
    obj = _ptr(self->ctx, db->obj);
    for(i=len=0; i<obj->len; i++)
        if (obj->item[i].key) len++;
    dbunlock(self->ctx);
    return len;
}

static int
PongoDict_contains(PongoDict *self, PyObject *key)
{
    dbtype_t *db, *k;
    _obj_t *obj;
    int i;
    int ret = 0;

    dblock(self->ctx);
    k = from_python(self->ctx, key);
    if (!PyErr_Occurred()) {
        db = _ptr(self->ctx, self->dbobj);
        obj = _ptr(self->ctx, db->obj);
        for(i=0; i<obj->len; i++) {
            if (dbeq(_ptr(self->ctx, obj->item[i].key), k, 1)) {
                ret = 1;
                break;
            }
        }
    }
    dbunlock(self->ctx);
    return ret;
}

static PyObject *
PongoDict_get(PongoDict *self, PyObject *args, PyObject *kwargs)
{
    PyObject *key, *dflt = NULL;
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
            r = dbobject_multi(SELF_CTX_AND_DBOBJ, k, multi_GET, &v, 0);
            if (r == 0) {
                ret = to_python(self->ctx, v, 1);
            } else if (dflt) {
                Py_INCREF(dflt);
                ret = dflt;
            } else {
                PyErr_SetObject(PyExc_KeyError, key);
            }
        }
#if 0
        else if (dbobject_getitem(SELF_CTX_AND_DBOBJ, k, &v) < 0) {
            dbfree(self->ctx, k);
            if (dflt) {
                Py_INCREF(dflt);
                ret = dflt;
            } else {
                PyErr_SetObject(PyExc_KeyError, key);
            }
        } else {
            ret = to_python(self->ctx, v, 1);
        }
#endif
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
    dbtype_t *k, *v;
    int sync = SYNC;
    char *kwlist[] = {"key", "value", "sep", "sync", NULL};
    char *sep = ".";

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|si:set", kwlist,
                &key, &value, &sep, &sync))
        return NULL;

    dblock(self->ctx);
    if (PyString_Check(key) || PyUnicode_Check(key)) {
        klist = PyObject_CallMethod(key, "split", "s", sep);
        k = from_python(self->ctx, klist);
        Py_XDECREF(klist);
    } else {
        k = from_python(self->ctx, key);
    }
    v = from_python(self->ctx, value);
    Py_XDECREF(klist);
    if (!PyErr_Occurred()) {
        if (k->type == List) {
            if (dbobject_multi(SELF_CTX_AND_DBOBJ, k, multi_SET, &v, sync) == 0) {
                ret = Py_None;
            } else {
                PyErr_SetObject(PyExc_KeyError, key);
            }
        } else if (dbobject_setitem(SELF_CTX_AND_DBOBJ, k, v, sync) == 0) {
            ret = Py_None;
        } else {
            PyErr_SetObject(PyExc_KeyError, key);
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
    dbtype_t *k, *v;
    int sync = SYNC;
    char *kwlist[] = {"key", "default", "sync", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|Oi:pop", kwlist,
                &key, &dflt, &sync))
        return NULL;

    dblock(self->ctx);
    k = from_python(self->ctx, key);
    if (!PyErr_Occurred()) {
        if (dbobject_delitem(SELF_CTX_AND_DBOBJ, k, &v, SYNC) < 0) {
            dbfree(self->ctx, k);
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
    dbtype_t *db;
    _obj_t *obj;
    PyObject *ret = PyList_New(0);
    PyObject *item;
    int i;

    dblock(self->ctx);
    db = _ptr(self->ctx, self->dbobj);
    obj = _ptr(self->ctx, db->obj);
    for(i=0; i<obj->len; i++) {
        if (obj->item[i].key) {
            item = to_python(self->ctx, _ptr(self->ctx, obj->item[i].key), 1);
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
    dbtype_t *db;
    _obj_t *obj;
    PyObject *ret = PyList_New(0);
    PyObject *item;
    int i;

    dblock(self->ctx);
    db = _ptr(self->ctx, self->dbobj);
    obj = _ptr(self->ctx, db->obj);
    for(i=0; i<obj->len; i++) {
        if (obj->item[i].key) {
            item = to_python(self->ctx, _ptr(self->ctx, obj->item[i].value), 1);
            PyList_Append(ret, item);
            Py_DECREF(item);
        }
    }
    dbunlock(self->ctx);
    return ret;
}

static PyObject *
PongoDict_items(PongoDict *self) {
    dbtype_t *db;
    _obj_t *obj;
    PyObject *ret = PyList_New(0);
    PyObject *item, *k, *v;
    int i;

    dblock(self->ctx);
    db = _ptr(self->ctx, self->dbobj);
    obj = _ptr(self->ctx, db->obj);
    for(i=0; i<obj->len; i++) {
        if (obj->item[i].key) {
            k = to_python(self->ctx, _ptr(self->ctx, obj->item[i].key), 1);
            v = to_python(self->ctx, _ptr(self->ctx, obj->item[i].value), 1);
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
    dbtype_t *dict, *obj, *k;
    jsonctx_t *jctx;

    if (!PyArg_ParseTuple(args, "|s#s#:json", &key, &klen, &val, &vlen))
        return NULL;

    dblock(self->ctx);
    dict = _ptr(self->ctx, self->dbobj);
    jctx = json_init(self->ctx);
    if (key) {
        if (val) {
            // 2-arg form is dict.json('key', 'value')
            // inserts dict['key'] = json_parse('value')
            k = dbstring_new(self->ctx, key, klen);
            obj = json_parse(jctx, val, vlen);
            dbobject_setitem(SELF_CTX_AND_DBOBJ, k, obj, SYNC);
        } else {
            // 1-arg form is replace dict.items with parsed json
            obj = json_parse(jctx, key, klen);
            dict->obj = obj->obj;
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
        PyErr_Format(PyExc_Exception, "Unknown relop '%s'", rel);
        return NULL;
    }

    if (PyString_Check(path)) {
        path = PyObject_CallMethod(path, "split", "s", sep);
        decpath = 1;
    }

    if (!PySequence_Check(path)) {
        PyErr_Format(PyExc_Exception, "path must be a sequence");
        return NULL;
    }
    dblock(self->ctx);
    dbpath = from_python(self->ctx, path);
    if (decpath)
        Py_DECREF(path);
    if (dbpath->type == List) {
        dbvalue = from_python(self->ctx, value);
        if (!PyErr_Occurred()) {
            dbrslt = dbobject_new(self->ctx);
            dbobject_search(SELF_CTX_AND_DBOBJ, dbpath, -1, relop, dbvalue, dbrslt);
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
PongoDict_stats(PongoDict *self)
{
    dbtype_t *db;
    _obj_t *obj;
    PyObject *ret = NULL;
    uint32_t hash;
    int i, len, misplaced;

    dblock(self->ctx);
    db = _ptr(self->ctx, self->dbobj);
    obj = _ptr(self->ctx, db->obj);
    misplaced = len = 0;
    for(i=0; i<obj->len; i++) {
        if (obj->item[i].key) {
            len++;
            hash = dbhashval(_ptr(self->ctx, obj->item[i].key));
            if (hash % obj->len != i)
                misplaced++;
        }
    }
    ret = Py_BuildValue("(iii)", obj->len, len, misplaced);
    dbunlock(self->ctx);
    return ret;
}
static PyObject *
PongoDict_repr(PyObject *ob)
{
    PongoDict *self = (PongoDict*)ob;
    char buf[32];
    sprintf(buf, "0x%llx", self->dbobj);
    return PyString_FromFormat("PongoDict(%p, %s)", self->ctx, buf);
}

void PongoDict_Del(PyObject *ob)
{
    PongoDict *self = (PongoDict*)ob;
    printf("PongoDict_Del %p %08llx\n", self, self->dbobj);
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
    {"json",    (PyCFunction)PongoDict_json,         METH_VARARGS, NULL },
    {"search",  (PyCFunction)PongoDict_search,       METH_VARARGS, NULL },
    {"stats",   (PyCFunction)PongoDict_stats,        METH_NOARGS, NULL },
    { NULL, NULL },
};

static PyTypeObject PongoDict_Type = {
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

static PyObject *
pongo_open(PyObject *self, PyObject *args)
{
    char *filename;
    pgctx_t *ctx;
    uint32_t initsize = 0;

    if (!PyArg_ParseTuple(args, "s|i:open", &filename, &initsize))
        return NULL;

    ctx = dbfile_open(filename, initsize);
    return PongoDict_Proxy(ctx, ctx->data);
}

static PyObject *
pongo_close(PyObject *self, PyObject *args)
{
    PongoDict *data;

    if (!PyArg_ParseTuple(args, "O:close", &data))
        return NULL;
    if (Py_TYPE(data) != &PongoDict_Type) {
        PyErr_Format(PyExc_TypeError, "Argument must be PongoDict");
        return NULL;
    }

    if (data->dbobj != data->ctx->root->data) {
        PyErr_Warn(PyExc_RuntimeWarning,
                "The object passed to close was not the root data object");
    }

    dbfile_close(data->ctx);
    Py_RETURN_NONE;
}

static PyObject *
pongo_meta(PyObject *self, PyObject *args)
{
    PongoDict *data;

    if (!PyArg_ParseTuple(args, "O:meta", &data))
        return NULL;
    if (Py_TYPE(data) != &PongoDict_Type) {
        PyErr_Format(PyExc_TypeError, "Argument must be PongoDict");
        return NULL;
    }

    if (data->dbobj != data->ctx->root->data) {
        PyErr_Warn(PyExc_RuntimeWarning,
                "The object passed to close was not the root data object");
    }

    return PongoDict_Proxy(data->ctx, data->ctx->meta);
}

static PyObject *
pongo_atoms(PyObject *self, PyObject *args)
{
    PongoDict *data;
    int used, misplaced;
    int i;
    uint64_t item;
    uint32_t hash;
    _cache_t *cache;

    if (!PyArg_ParseTuple(args, "O:atoms", &data))
        return NULL;
    if (Py_TYPE(data) != &PongoDict_Type) {
        PyErr_Format(PyExc_TypeError, "Argument must be PongoDict");
        return NULL;
    }

    dblock(data->ctx);
    cache = _ptr(data->ctx, data->ctx->cache->cache);
    used = misplaced = 0;
    for(i=0; i<cache->len; i++) {
        item = cache->item[i];
        if (item) {
            used++;
            hash = dbhashval(_ptr(data->ctx, item));
            if (hash % cache->len != i)
                misplaced++;
        }
    }
    dbunlock(data->ctx);

    return Py_BuildValue("(Liiiii)", 
            _offset(data->ctx, data->ctx->cache), dbsize(data->ctx, cache),
            cache->len, cache->retry, used, misplaced);
}

static PyObject *
pongo_recache(PyObject *self, PyObject *args)
{
    PongoDict *data;
    int cachesz, linesz;

    if (!PyArg_ParseTuple(args, "Oii:recache", &data, &cachesz, &linesz))
        return NULL;
    if (Py_TYPE(data) != &PongoDict_Type) {
        PyErr_Format(PyExc_TypeError, "Argument must be PongoDict");
        return NULL;
    }
    dblock(data->ctx);
    dbcache_recache(data->ctx, cachesz, linesz);
    dbunlock(data->ctx);
    Py_RETURN_NONE;
}

static PyObject *
pongo__object(PyObject *self, PyObject *args)
{
    PongoDict *data;
    uint64_t offset;
    dbtype_t *db;

    if (!PyArg_ParseTuple(args, "OL:_object", &data, &offset))
        return NULL;
    if (Py_TYPE(data) != &PongoDict_Type) {
        PyErr_Format(PyExc_TypeError, "Argument must be PongoDict");
        return NULL;
    }

    db = _ptr(data->ctx, offset);
    return to_python(data->ctx, db, 1);
}

static PyObject *
pongo__info(PyObject *self, PyObject *args)
{
    PongoDict *data;
    uint64_t offset;

    if (!PyArg_ParseTuple(args, "O:_info", &data, &offset))
        return NULL;
    if (Py_TYPE(data) != &PongoDict_Type) {
        PyErr_Format(PyExc_TypeError, "Argument must be PongoDict");
        return NULL;
    }
    dbmem_info(data->ctx);
    Py_RETURN_NONE;
}

static PyObject *
pongo_gc(PyObject *self, PyObject *args)
{
    PyObject *ret = Py_None;
    PongoDict *data;
    gcstats_t _stats;
    gcstats_t *stats = &_stats;
    int getstats = 1;

    if (!PyArg_ParseTuple(args, "O|i:gc", &data, &getstats))
        return NULL;

    if (Py_TYPE(data) != &PongoDict_Type) {
        PyErr_Format(PyExc_TypeError, "Argument must be PongoDict");
        return NULL;
    }

    if (data->dbobj != data->ctx->root->data) {
        PyErr_Warn(PyExc_RuntimeWarning,
                "The object passed to gc was not the root data object");
    }

    memset(stats, 0, sizeof(*stats));
    if (!getstats) stats = NULL;
    db_gc(data->ctx, stats);
    if (stats) {
        ret = Py_BuildValue("(iiii)",
                stats->before.num, stats->before.size,
                stats->after.num, stats->after.size);
    }
    Py_INCREF(ret);
    return ret;
}
static PyMethodDef _pongo_methods[] = {
    { "open",   (PyCFunction)pongo_open, METH_VARARGS, NULL },
    { "close",  (PyCFunction)pongo_close, METH_VARARGS, NULL },
    { "meta",   (PyCFunction)pongo_meta, METH_VARARGS, NULL },
    { "atoms",  (PyCFunction)pongo_atoms, METH_VARARGS, NULL },
    { "recache",(PyCFunction)pongo_recache, METH_VARARGS, NULL },
    { "_object",(PyCFunction)pongo__object, METH_VARARGS, NULL },
    { "_info",  (PyCFunction)pongo__info, METH_VARARGS, NULL },
    { "gc",     (PyCFunction)pongo_gc, METH_VARARGS, NULL },
    { NULL, NULL },
};

PyMODINIT_FUNC init_pongo(void)
{
    PyObject *m;

    PyDateTime_IMPORT;
    m = Py_InitModule("_pongo", _pongo_methods);
    PyType_Ready(&PongoList_Type);
    PyType_Ready(&PongoDict_Type);

    Py_INCREF(&PongoList_Type);
    Py_INCREF(&PongoDict_Type);
    PyModule_AddObject(m, "PongoList", (PyObject*)&PongoList_Type);
    PyModule_AddObject(m, "PongoDict", (PyObject*)&PongoDict_Type);

    log_init(NULL, LOG_DEBUG);
}

/*
 * vim: ts=4 sts=4 sw=4 expandtab:
 */
