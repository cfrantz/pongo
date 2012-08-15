#include "pongo.h"
#include <pongo/bonsai.h>
#include <datetime.h>

#ifdef WANT_UUID_TYPE
static PyTypeObject *uuid_class;
static PyObject *uuid_constructor;
#endif
PyObject *pongo_id;
PyObject *pongo_newkey = Py_None;

static dbtype_t *
pongo_newkey_helper(pgctx_t *ctx, dbtype_t *value)
{
    PyObject *ob;
    ob = PyObject_CallFunction(pongo_newkey, "O", to_python(ctx, value, 1));
    return from_python(ctx, ob);
}

typedef struct {
    int proxy;
    dbtag_t type;
    PyObject *ob;
} tphelper_t;

static void
to_python_helper(pgctx_t *ctx, dbtype_t *node, void *user)
{
    tphelper_t *h = (tphelper_t*)user;
    PyObject *k, *v;

    if (h->type == Collection) {
        k = to_python(ctx, _ptr(ctx, node->key), h->proxy);
        v = to_python(ctx, _ptr(ctx, node->value), h->proxy);
        PyDict_SetItem(h->ob, k, v);
        Py_DECREF(k); Py_DECREF(v);
    } else {
        //FIXME: exception
    }
}

PyObject *
to_python(pgctx_t *ctx, dbtype_t *db, int proxy)
{
    PyObject *ob = NULL;
    PyObject *k, *v;
    _list_t *list;
    _obj_t *obj;
    struct tm tm;
    time_t time;
    long usec;
    int i;
    int64_t ival;
    tphelper_t h;

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
#ifdef WANT_UUID_TYPE
        case Uuid:
            ob = PyObject_CallFunction(uuid_constructor, "Os#", Py_None, db->uuval, 16);
            break;
#endif
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
                pidcache_put(ctx, ob, db);
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
                pidcache_put(ctx, ob, db);
            } else {
                if (proxy == -1) proxy = 1;
                obj = _ptr(ctx, db->obj);
                ob = PyDict_New();
                for(i=0; i<obj->len; i++) {
                    k = to_python(ctx, _ptr(ctx, obj->item[i].key), proxy);
                    v = to_python(ctx, _ptr(ctx, obj->item[i].value), proxy);
                    PyDict_SetItem(ob, k, v);
                    Py_DECREF(k); Py_DECREF(v);
                }
            }
            break;
        case Cache:
            // The cache is a collection
        case Collection:
            if (proxy == 1) {
                ob = PongoCollection_Proxy(ctx, db);
                pidcache_put(ctx, ob, db);
            } else {
                if (proxy == -1) proxy = 1;
                h.proxy = proxy;
                h.type = Collection;
                h.ob = ob = PyDict_New();
                bonsai_foreach(ctx, _ptr(ctx, db->obj), to_python_helper, &h);
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

dbtype_t *
from_python(pgctx_t *ctx, PyObject *ob)
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
#ifdef WANT_UUID_TYPE
    } else if (PyObject_TypeCheck(ob, uuid_class)) {
        ob = PyObject_CallMethod(ob, "get_bytes", NULL);
        PyString_AsStringAndSize(ob, &buf, &length);
        db = dbuuid_new(ctx, (uint8_t*)buf);
#endif
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
    } else if (Py_TYPE(ob) == &PongoCollection_Type) {
        // Resolve proxy types back to their original dbtype
        PongoCollection *p = (PongoCollection*)ob;
        db = _ptr(p->ctx, p->dbcoll);
    } else {
        // FIXME: Unknown object type
        printf("Type = %p", Py_TYPE(ob));
        PyErr_SetObject(PyExc_TypeError, ob);
        db = NULL;
    }
    return db;
}

void pongo_abort(const char *msg)
{
    PyErr_Format(PyExc_Exception, "Abort: %s", msg);
}

static PyObject *
pongo_open(PyObject *self, PyObject *args)
{
    char *filename;
    pgctx_t *ctx;
    uint32_t initsize = 0;

    if (!PyArg_ParseTuple(args, "s|i:open", &filename, &initsize))
        return NULL;

    ctx = dbfile_open(filename, initsize);
    pidcache_new(ctx);
    // Create a python proxy of the root data object
    return PongoCollection_Proxy(ctx, ctx->data);
}

int
pongo_check(PongoCollection *data)
{
    if ((Py_TYPE(data) != &PongoDict_Type && Py_TYPE(data) != &PongoCollection_Type) ||
        data->dbcoll != data->ctx->root->data) {
            PyErr_Format(PyExc_TypeError, "Argument must be a Pongo root object");
            return -1;
    }
    return 0;
}

static PyObject *
pongo_close(PyObject *self, PyObject *args)
{
    PongoCollection *data;

    if (!PyArg_ParseTuple(args, "O:close", &data))
        return NULL;
    if (pongo_check(data))
        return NULL;

    dblock(data->ctx);
    pidcache_destroy(data->ctx);
    dbunlock(data->ctx);
    dbfile_close(data->ctx);
    Py_RETURN_NONE;
}

static PyObject *
pongo_meta(PyObject *self, PyObject *args)
{
    PongoCollection *data;
    pgctx_t *ctx;
    const char *key;
    PyObject *value = NULL, *ret = Py_None;

    if (!PyArg_ParseTuple(args, "Os|O:meta", &data, &key, &value))
        return NULL;
    if (pongo_check(data))
        return NULL;

    ctx = data->ctx;
    if (!strcmp(key, "chunksize")) {
        ret = PyLong_FromLongLong(ctx->root->meta.chunksize);
        if (value && value != Py_None) ctx->root->meta.chunksize = PyInt_AsLong(value);
    } else if (!strcmp(key, "id")) {
        ret = to_python(ctx, _ptr(ctx, ctx->root->meta.id), 0);
        if (value) ctx->root->meta.id = _offset(ctx, from_python(ctx, value));
#ifdef WANT_UUID_TYPE
    } else if (!strcmp(key, ".uuid_class")) {
        ret = (PyObject*)uuid_class;
        if (value) uuid_class = (PyTypeObject*)value;
    } else if (!strcmp(key, ".uuid_constructor")) {
        ret = (PyObject*)uuid_constructor;
        if (value) uuid_constructor = value;
#endif
    } else if (!strcmp(key, ".newkey")) {
        ret = pongo_newkey;
        if (value) {
            pongo_newkey = value;
            if (value == Py_None) {
#ifdef WANT_UUID_TYPE
                ctx->newkey = _newkey;
#else
                ctx->newkey = NULL;
#endif
            } else {
                ctx->newkey = pongo_newkey_helper;
            }
        }
    } else {
        PyErr_Format(PyExc_Exception, "Unknown meta key %s", key);
        ret = NULL;
    }

    return ret;
}

static PyObject *
pongo_atoms(PyObject *self, PyObject *args)
{
    PyObject *ret;
    PongoCollection *data;

    if (!PyArg_ParseTuple(args, "O:atoms", &data))
        return NULL;
    if (pongo_check(data))
        return NULL;

    dblock(data->ctx);
    // Create the collection directly because the cache is
    // already accounted for by pongogc, so we don't need to
    // have the proxy reference inserted into the pidcache
    ret = PongoCollection_Proxy(data->ctx, data->ctx->cache);
    dbunlock(data->ctx);

    return ret;
}

static PyObject *
pongo_pidcache(PyObject *self, PyObject *args)
{
    PyObject *ret;
    PongoCollection *data;
    dbtype_t *pidcache;

    if (!PyArg_ParseTuple(args, "O:atoms", &data))
        return NULL;
    if (pongo_check(data))
        return NULL;

    dblock(data->ctx);
    pidcache = _ptr(data->ctx, data->ctx->root->pidcache);
    // Create the collection directly because the pidcache is
    // already accounted for by pongogc, so we don't need to
    // have the proxy reference inserted into the pidcache
    ret = PongoCollection_Proxy(data->ctx, pidcache);
    dbunlock(data->ctx);

    return ret;
}

static PyObject *
pongo__object(PyObject *self, PyObject *args)
{
    PongoCollection *data;
    uint64_t offset;
    dbtype_t *db;

    if (!PyArg_ParseTuple(args, "OL:_object", &data, &offset))
        return NULL;
    if (pongo_check(data))
        return NULL;

    db = _ptr(data->ctx, offset);
    return to_python(data->ctx, db, 1);
}

static PyObject *
pongo__info(PyObject *self, PyObject *args)
{
    PongoCollection *data;
    uint64_t offset;

    if (!PyArg_ParseTuple(args, "O:_info", &data, &offset))
        return NULL;
    if (pongo_check(data))
        return NULL;

    dbmem_info(data->ctx);
    Py_RETURN_NONE;
}

static PyObject *
pongo_gc(PyObject *self, PyObject *args)
{
    PyObject *ret = Py_None;
    PongoCollection *data;
    gcstats_t _stats;
    gcstats_t *stats = &_stats;
    int getstats = 1;

    if (!PyArg_ParseTuple(args, "O|i:gc", &data, &getstats))
        return NULL;
    if (pongo_check(data))
        return NULL;

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
    { "pidcache",  (PyCFunction)pongo_pidcache, METH_VARARGS, NULL },
    { "_object",(PyCFunction)pongo__object, METH_VARARGS, NULL },
    { "_info",  (PyCFunction)pongo__info, METH_VARARGS, NULL },
    { "gc",     (PyCFunction)pongo_gc, METH_VARARGS, NULL },
    { NULL, NULL },
};

void pongo_atexit(void)
{
    int i;
    pgctx_t *ctx;
    for(i=0; i<NR_DB_CONTEXT; i++) {
        ctx = dbctx[i];
        if (ctx) {
            dblock(ctx);
            pidcache_destroy(ctx);
            dbunlock(ctx);
        }
    }
}

PyMODINIT_FUNC init_pongo(void)
{
    PyObject *m;
    PyObject *uumod;

    PyDateTime_IMPORT;
    m = Py_InitModule("_pongo", _pongo_methods);
    PyType_Ready(&PongoList_Type);
    PyType_Ready(&PongoDict_Type);
    PyType_Ready(&PongoCollection_Type);

    Py_INCREF(&PongoList_Type);
    Py_INCREF(&PongoDict_Type);
    Py_INCREF(&PongoCollection_Type);
    PyModule_AddObject(m, "PongoList", (PyObject*)&PongoList_Type);
    PyModule_AddObject(m, "PongoDict", (PyObject*)&PongoDict_Type);
    PyModule_AddObject(m, "PongoCollection", (PyObject*)&PongoCollection_Type);

    pongo_id = PyObject_CallFunction((PyObject*)&PyBaseObject_Type, NULL);
    PyModule_AddObject(m, "id", pongo_id);

#ifdef WANT_UUID_TYPE
    uumod = PyImport_ImportModule("uuid");
    uuid_constructor = PyObject_GetAttrString(uumod, "UUID");
    uuid_class = (PyTypeObject*)uuid_constructor;
#endif

    Py_AtExit(pongo_atexit);

    log_init(NULL, LOG_DEBUG);
}

/*
 * vim: ts=4 sts=4 sw=4 expandtab:
 */
