#ifndef PYTHON_PONGO_H
#define PYTHON_PONGO_H
#include <Python.h>
#include <structmember.h>

#include <pongo/log.h>
#include <pongo/dbmem.h>
#include <pongo/dbtypes.h>
#include <pongo/pidcache.h>
#include <pongo/json.h>


typedef struct {
    PyObject_HEAD
    pgctx_t *ctx;
    dbtype_t dblist;
} PongoList;

typedef struct {
    PyObject_HEAD
    pgctx_t *ctx;
    dbtype_t dbobj;
} PongoDict;

typedef struct {
    PyObject_HEAD
    pgctx_t *ctx;
    dbtype_t dbcoll;
    dbtype_t index;
    PyObject *index_ob;
} PongoCollection;

typedef struct {
    PyObject_HEAD
    pgctx_t *ctx;
    dbtype_t dbptr;
} PongoPointer;

typedef struct {
    PyObject_HEAD
    pgctx_t *ctx;
    dbtype_t dbptr;
    uint32_t pos, len;
    dbtype_t stack[64];
    int depth;
} PongoIter;

#define SELF_CTX_AND_DBLIST self->ctx, self->dblist
#define SELF_CTX_AND_DBOBJ self->ctx, self->dbobj
#define SELF_CTX_AND_DBCOLL self->ctx, self->dbcoll

extern PyTypeObject PongoList_Type;
extern PyTypeObject PongoDict_Type;
extern PyTypeObject PongoCollection_Type;
extern PyTypeObject PongoPointer_Type;
extern PyTypeObject PongoIter_Type;
extern PyObject *pongo_id;

extern PyObject* PongoList_Proxy(pgctx_t *ctx, dbtype_t db);
extern PyObject* PongoDict_Proxy(pgctx_t *ctx, dbtype_t db);
extern PyObject* PongoCollection_Proxy(pgctx_t *ctx, dbtype_t db);
extern PyObject* PongoPointer_Proxy(pgctx_t *ctx, dbtype_t db);
extern PyObject* PongoIter_Iter(PyObject *ob);

#define TP_PROXY	0x0001
#define TP_PROXYCHLD	0x0002
#define TP_NODEKEY	0x0004
#define TP_NODEVAL	0x0008
extern PyObject *to_python(pgctx_t *ctx, dbtype_t db, int flags);
extern dbtype_t from_python(pgctx_t *ctx, PyObject *ob);
extern int pongo_check(PongoCollection *data);

// vim: ts=4 sts=4 sw=4 expandtab:
#endif
