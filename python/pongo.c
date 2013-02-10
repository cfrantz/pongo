#include "pongo.h"
#include <pongo/bonsai.h>
#include <datetime.h>

#ifdef WANT_UUID_TYPE
static PyTypeObject *uuid_class;
static PyObject *uuid_constructor;
#endif
PyObject *pongo_id;
PyObject *pongo_utcnow;
PyObject *pongo_newkey = Py_None;

static dbtype_t
pongo_newkey_helper(pgctx_t *ctx, dbtype_t value)
{
    // FIXME: something wrong here.
    PyObject *ob;
    ob = PyObject_CallFunction(pongo_newkey, "(N)", to_python(ctx, value, 1));
    if (ob) {
        value = from_python(ctx, ob);
        Py_DECREF(ob);
    } else {
        value = DBNULL;
    }
    return value;
}

typedef struct {
    int flags;
    dbtag_t type;
    PyObject *ob;
} tphelper_t;

static void
to_python_helper(pgctx_t *ctx, dbtype_t node, void *user)
{
    tphelper_t *h = (tphelper_t*)user;
    PyObject *kv;

    if (h->type == Collection || h->type == MultiCollection) {
        kv = to_python(ctx, node, h->flags);
        PyDict_SetItem(h->ob, 
                PyTuple_GET_ITEM(kv, 0),
                PyTuple_GET_ITEM(kv, 1));
        Py_DECREF(kv);
    } else {
        //FIXME: exception
    }
}

PyObject *
to_python(pgctx_t *ctx, dbtype_t db, int flags)
{
    dbtag_t type;
    dbval_t *dv = NULL;
    PyObject *ob = NULL;
    PyObject *k, *v;
    epstr_t ea;
    epfloat_t fa;
    char *ma = NULL;
    uint32_t len = 0;
    _list_t *list;
    _obj_t *obj;
    struct tm tm;
    time_t time;
    long usec;
    int i;
    int64_t ival;
    tphelper_t h;

    if (db.all == 0)
        Py_RETURN_NONE;

    type = db.type;
    if (type == ByteBuffer || type == String) {
        ea.all = db.all;
        len = ea.len;
        ea.val[len] = 0;
        ma = (char*)ea.val;
    } else if (isPtr(type)) {
        dv = dbptr(ctx, db);
        type = dv->type;
        if (type == ByteBuffer || type == String) {
            len = dv->len;
            ma = (char*)dv->sval;
        }
    }


    switch(type) {
        case Boolean:
            ob = db.val ? Py_True : Py_False;
            Py_INCREF(ob);
            break;
        case Int:
            ival = db.val;
            if (ival < LONG_MIN || ival > LONG_MAX) {
                ob = PyLong_FromLongLong(ival);
            } else {
                ob = PyInt_FromLong((long)ival);
            }
            break;
        case Float:
            fa.ival = (int64_t)db.val << 4;
            ob = PyFloat_FromDouble(fa.fval);
            break;
#ifdef WANT_UUID_TYPE
        case Uuid:
            ob = PyObject_CallFunction(uuid_constructor, "Os#", Py_None, dv->uuval, 16);
            break;
#endif
        case ByteBuffer:
            ob = PyString_FromStringAndSize(ma, len);
            break;
        case String:
            ob = PyUnicode_FromStringAndSize(ma, len);
            break;
        case Datetime:
            time = db.val / 1000000LL;
            usec = db.val % 1000000LL;
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
            if (flags & TP_PROXY) {
                ob = PongoList_Proxy(ctx, db);
                pidcache_put(ctx, ob, db);
            } else {
                if (flags & TP_PROXYCHLD) flags = (flags & ~TP_PROXYCHLD) | TP_PROXY;
                list = dbptr(ctx, dv->list);
                ob = PyList_New(list->len);
                for(i=0; i<list->len; i++) {
                    v = to_python(ctx, list->item[i], flags);
                    PyList_SET_ITEM(ob, i, v);
                    // Don't need to decref v since SET_ITEM steals the reference
                }
            }
            break;
        case Object:
            if (flags & TP_PROXY) {
                ob = PongoDict_Proxy(ctx, db);
                pidcache_put(ctx, ob, db);
            } else {
                if (flags & TP_PROXYCHLD) flags = (flags & ~TP_PROXYCHLD) | TP_PROXY;
                obj = dbptr(ctx, dv->obj);
                ob = PyDict_New();
                for(i=0; i<obj->len; i++) {
                    k = to_python(ctx, obj->item[i].key, flags);
                    v = to_python(ctx, obj->item[i].value, flags);
                    PyDict_SetItem(ob, k, v);
                    Py_DECREF(k); Py_DECREF(v);
                }
            }
            break;
        case Cache:
            // The cache is a collection
        case Collection:
        case MultiCollection:
            if (flags & TP_PROXY) {
                ob = PongoCollection_Proxy(ctx, db);
                pidcache_put(ctx, ob, db);
            } else {
                if (flags & TP_PROXYCHLD) flags = (flags & ~TP_PROXYCHLD) | TP_PROXY;
                h.flags = flags;
                h.type = Collection;
                h.ob = ob = PyDict_New();
                bonsai_foreach(ctx, dv->obj, to_python_helper, &h);
            }
            break;

        case _BonsaiNode:
        case _BonsaiMultiNode:
            k = v = NULL;
            if (flags & TP_NODEKEY) {
                k = to_python(ctx, dv->key, flags & ~(TP_NODEKEY|TP_NODEVAL));
                ob = k;
            }
            if (flags & TP_NODEVAL) {
                if (type == _BonsaiMultiNode) {
                    v = PyTuple_New(dv->nvalue);
                    for(i=0; i<dv->nvalue; i++) {
                        ob = to_python(ctx, dv->values[i], flags & ~(TP_NODEKEY|TP_NODEVAL));
                        PyTuple_SET_ITEM(v, i, ob);
                        // Don't need to decref ob since SET_ITEM steals the reference
                    }
                } else {
                    v = to_python(ctx, dv->value, flags & ~(TP_NODEKEY|TP_NODEVAL));
                }
                ob = v;
            }
            if (k && v) {
                ob = PyTuple_Pack(2, k, v);
                Py_DECREF(k); Py_DECREF(v);
            }
            break;
        default:
            PyErr_Format(PyExc_Exception, "Cannot handle dbtype %d", type);
    }
    return ob;
}

static int sequence_cb(pgctx_t *ctx, int i, dbtype_t *item, void *user)
{
    PyObject *seq = (PyObject*)user;
    PyObject *sqitem = PySequence_GetItem(seq, i);
    *item = from_python(ctx, sqitem);
    Py_DECREF(sqitem);
    if (PyErr_Occurred()) return -1;
    return 0;
}

static int mapping_cb(pgctx_t *ctx, int i, dbtype_t *key, dbtype_t *value, void *user)
{
    PyObject *map = (PyObject*)user;
    PyObject *item = PySequence_GetItem(map, i);
    PyObject *k = PySequence_GetItem(item, 0);
    PyObject *v = PySequence_GetItem(item, 1);
    *key = from_python(ctx, k);
    *value = from_python(ctx, v);
    Py_DECREF(k); Py_DECREF(v);
    Py_DECREF(item);
    if (PyErr_Occurred()) return -1;
    return 0;
}

static int itermapping_cb(pgctx_t *ctx, int i, dbtype_t *key, dbtype_t *value, void *user)
{
    PyObject *iter = (PyObject*)user;
    PyObject *item = PyIter_Next(iter);
    PyObject *k = PySequence_GetItem(item, 0);
    PyObject *v = PySequence_GetItem(item, 1);
    *key = from_python(ctx, k);
    *value = from_python(ctx, v);
    Py_DECREF(k); Py_DECREF(v);
    Py_DECREF(item);
    if (PyErr_Occurred()) return -1;
    return 0;
}

dbtype_t
from_python(pgctx_t *ctx, PyObject *ob)
{
    dbtype_t db;
    char *buf;
    Py_ssize_t length;
    PyObject *items;
    struct tm tm;
    long usec;
    //int i;
    
    if (PyObject_HasAttrString(ob, "__topongo__")) {
        ob = PyObject_CallMethod(ob, "__topongo__", NULL);
        if (PyErr_Occurred())
            return DBNULL;
    }
    if (ob == Py_None) {
        db = DBNULL;
    } else if (ob == pongo_id) {
        db = dbuuid_new(ctx, NULL);
    } else if (ob == pongo_utcnow) {
        db = dbtime_now(ctx);
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
        // FIXME:
        //db = dbbuffer_new(ctx, buf, length);
        db = dbstring_new(ctx, buf, length);
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
            Py_XDECREF(items);
        } else {
            // mapping object implements iterator protocol
            // don't have to decref the iterator object cuz it self-decrefs
            // upon StopIteration
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
        db = p->dblist;
    } else if (Py_TYPE(ob) == &PongoDict_Type) {
        // Resolve proxy types back to their original dbtype
        PongoDict *p = (PongoDict*)ob;
        db = p->dbobj;
    } else if (Py_TYPE(ob) == &PongoCollection_Type) {
        // Resolve proxy types back to their original dbtype
        PongoCollection *p = (PongoCollection*)ob;
        db = p->dbcoll;
    } else {
        // FIXME: Unknown object type
        PyErr_SetObject(PyExc_TypeError, (PyObject*)Py_TYPE(ob));
        db = DBNULL;
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
    dblock(ctx);
    pidcache_new(ctx);
    dbunlock(ctx);
    // Create a python proxy of the root data object
    return PongoCollection_Proxy(ctx, ctx->data);
}

int
pongo_check(PongoCollection *data)
{
    if ((Py_TYPE(data) != &PongoDict_Type && Py_TYPE(data) != &PongoCollection_Type) ||
        data->dbcoll.all != data->ctx->root->data.all) {
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
    dblock(ctx);
    if (!strcmp(key, "chunksize")) {
        ret = PyLong_FromLongLong(ctx->root->meta.chunksize);
        if (value && value != Py_None) ctx->root->meta.chunksize = PyInt_AsLong(value);
    } else if (!strcmp(key, "id")) {
        ret = to_python(ctx, ctx->root->meta.id, 0);
        if (value) ctx->root->meta.id = from_python(ctx, value);
    } else if (!strcmp(key, ".sync")) {
        ret = PyInt_FromLong(ctx->sync);
        if (value && value != Py_None) ctx->sync = PyInt_AsLong(value);
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
    dbunlock(ctx);

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
    dbtype_t pidcache;

    if (!PyArg_ParseTuple(args, "O:atoms", &data))
        return NULL;
    if (pongo_check(data))
        return NULL;

    dblock(data->ctx);
    pidcache = data->ctx->root->pidcache;
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
    PyObject *ob;
    PongoCollection *data;
    uint64_t offset;
    dbtype_t db;

    if (!PyArg_ParseTuple(args, "OL:_object", &data, &offset))
        return NULL;
    if (pongo_check(data))
        return NULL;

    db.all = offset;
    dblock(data->ctx);
    ob = to_python(data->ctx, db, 1);
    dbunlock(data->ctx);
    return ob;
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
pongo__show(PyObject *self, PyObject *args)
{
    PongoCollection *data;
    dbtype_t coll;

    if (!PyArg_ParseTuple(args, "O:_info", &data))
        return NULL;

    coll.ptr = dbptr(data->ctx, data->dbcoll);
    bonsai_show(data->ctx,  coll.ptr->obj, 0);
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
    int complete = 0;

    if (!PyArg_ParseTuple(args, "O|ii:gc", &data, &complete, &getstats))
        return NULL;
    if (pongo_check(data))
        return NULL;

    memset(stats, 0, sizeof(*stats));
    if (!getstats) stats = NULL;
    db_gc(data->ctx, complete, stats);
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
    { "_show",  (PyCFunction)pongo__show, METH_VARARGS, NULL },
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

    pongo_utcnow = PyObject_CallFunction((PyObject*)&PyBaseObject_Type, NULL);
    PyModule_AddObject(m, "utcnow", pongo_utcnow);

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
