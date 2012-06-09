#ifndef PONGO__DBTYPES_H
#define PONGO__DBTYPES_H

#include <pongo/stdtypes.h>
#include <pongo/pmem.h>

#pragma pack(1)
typedef enum {
	Undefined =	-1,
	Null =		0,
	Boolean =	1,
	Int =		2,
	Float =		3,
	ByteBuffer =	4,
	String =	5,
	Uuid =		6,
	Datetime =	7,
	List =		8,
	Object =	9,
	Cache =		10,
	_InternalList=	11,
	_InternalObj=	12,
} dbtag_t;

#define DBROOT_SIG "PongoDB"
#define NR_SMALL_INTS 261
typedef struct _dbroot {
	MEMBLOCK_HEAD
	uint32_t _pad0;
	uint32_t version;
	uint64_t meta;
	uint64_t data;
	uint64_t cache;
	uint64_t booleans[2];
	uint64_t integers;
	uint32_t lock;
	uint32_t resize_lock;
	struct {
		int64_t gc_time;
		int64_t gc_pid;
	} gc;
	uint8_t _pad1[]; // Padded out to 4k
} dbroot_t;
	
typedef struct {
	dbtag_t type;
	uint32_t _pad;
	uint64_t bval;
} dbboolean_t;

typedef struct {
	dbtag_t type;
	uint32_t _pad;
	int64_t ival;
} dbint_t;

typedef struct {
	dbtag_t type;
	uint32_t _pad;
	double fval;
} dbfloat_t;

typedef struct {
	dbtag_t type;
	uint32_t len;
	uint32_t hash;
	uint8_t sval[];
} dbstring_t;

typedef struct {
	dbtag_t type;
	uint8_t uuval[16];
} dbuuid_t;

typedef struct {
	dbtag_t type;
	uint32_t _pad;
	int64_t utctime;
} dbtime_t;

typedef struct {
	dbtag_t type;
	uint32_t len;
	uint32_t pad[2];
	uint64_t item[];
} _list_t;

typedef struct {
	dbtag_t type;
	uint64_t list;
} dblist_t;

typedef struct  {
	uint64_t key, value;
} _objitem_t;

typedef struct {
	dbtag_t type;
	uint32_t len;
	uint32_t retry;
	uint32_t pad[1];
	_objitem_t item[];
} _obj_t;

typedef struct {
	dbtag_t type;
	uint64_t obj;
} dbobject_t;

typedef struct {
	uint32_t len;
	uint32_t retry;
	uint64_t item[];
} _cache_t;

typedef struct {
	dbtag_t type;
	uint64_t cache;
} dbcache_t;

#ifndef WIN32
typedef union {
	dbtag_t type;
	dbboolean_t;
	dbint_t;
	dbfloat_t;
	dbstring_t;
	dbuuid_t;
	dbtime_t;
	dblist_t;
	dbobject_t;
	dbcache_t;
} dbtype_t;
#else
typedef struct {
	dbtag_t type;
	union {
		struct {
			uint32_t _pad;
			union {
				int64_t bval;
				int64_t ival;
				int64_t utctime;
				double fval;
			};
		};
		struct {
			uint32_t len;
			uint32_t hash;
			uint8_t sval[];
		};
		uint8_t uuval[16];
		uint64_t list;
		uint64_t obj;
		uint64_t cache;
	};
} dbtype_t;
#endif
#pragma pack()

#endif
