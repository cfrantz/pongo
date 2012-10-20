#ifndef PYTHON_PONGO_H
#define PYTHON_PONGO_H
#include <Python.h>
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
} PongoCollection;

#define SELF_CTX_AND_DBLIST self->ctx, self->dblist
#define SELF_CTX_AND_DBOBJ self->ctx, self->dbobj
#define SELF_CTX_AND_DBCOLL self->ctx, self->dbcoll

extern PyTypeObject PongoList_Type;
extern PyTypeObject PongoDict_Type;
extern PyTypeObject PongoCollection_Type;
extern PyObject *pongo_id;

extern PyObject* PongoList_Proxy(pgctx_t *ctx, dbtype_t db);
extern PyObject* PongoDict_Proxy(pgctx_t *ctx, dbtype_t db);
extern PyObject* PongoCollection_Proxy(pgctx_t *ctx, dbtype_t db);

extern dbtype_t from_python(pgctx_t *ctx, PyObject *ob);
extern PyObject *to_python(pgctx_t *ctx, dbtype_t db, int proxy);
extern int pongo_check(PongoCollection *data);

#endif
