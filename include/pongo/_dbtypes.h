#ifndef PONGO__DBTYPES_H
#define PONGO__DBTYPES_H

#include <pongo/stdtypes.h>
#include <pongo/pmem.h>

#pragma pack(1)
typedef enum {
	Undefined =	-1,
	// Primitive types (0-127)
	Null =		0x00,
	Boolean =	0x01,
	Int =		0x02,
	Datetime =	0x03,
	Uuid =		0x04,
	Float =		0x05,
	ByteBuffer =0x06,
	String =	0x07,

	// Container types (129-...)
	List =		0x81,
	Object =	0x82,
	Collection =	0x83,
	Cache =		0x84,

	// Maintenence types for containers (193-255)
	_InternalList=	0xc1,
	_InternalObj=	0xc2,
	_BonsaiNode=	0xc3
} dbtag_t;

#define DBROOT_SIG "PongoDB"
typedef struct _dbroot {
    uint8_t signature[16];      // 0    +16 bytes
    uint16_t version[4];        // 16   +8 bytes
    uint64_t _pad0;             // 24   +8 bytes
	uint64_t heap;				// 32   +8 bytes
	uint64_t data;				// 40   +8 bytes
	uint64_t cache;				// 48	+8 bytes
	uint64_t pidcache;			// 56   +8 bytes
	uint64_t booleans[2];		// 64	+16 bytes
	uint64_t lock;				// 80   +8 bytes
	uint64_t resize; 	        // 88   +8 bytes
	struct {
		int64_t gc_time;
		int64_t gc_pid;
	} gc;                       // 96  +16 bytes
	uint8_t _pad1[3072-112];	// 112
	struct __meta {
		uint64_t chunksize;		// 3072 + 8 bytes
		uint64_t id;			// 3080 + 8 bytes
	} meta;
	uint8_t _pad2[1024-sizeof(struct __meta)];
						// 4096 bytes
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
	uint32_t _uupad[3];
	uint8_t uuval[16];
} dbuuid_t;

typedef struct {
	dbtag_t type;
	uint32_t _pad;
	int64_t utctime;
} dbtime_t;

typedef struct {
	dbtag_t type;
	uint32_t _pad[2];
	uint32_t len;
	uint64_t item[];
} _list_t;

typedef struct {
	dbtag_t type;
	uint32_t _pad;
	uint64_t list;
    uint8_t _extra[48];
} dblist_t;

typedef struct  {
	uint64_t key, value;
} _objitem_t;

typedef struct {
	dbtag_t type;
	uint32_t _pad[2];
	uint32_t len;
	_objitem_t item[];
} _obj_t;

typedef struct {
	dbtag_t type;
	uint32_t _pad;
	uint64_t obj;
    uint8_t _extra[48];
} dbobject_t;

typedef struct {
	dbtag_t type;
	uint32_t refcnt; // used only by pidcache
	uint64_t obj;
    uint8_t _extra[48];
} dbcollection_t;

typedef struct {
	dbtag_t type;
	uint32_t _pad;
	uint64_t cache;
    uint8_t _extra[48];
} dbcache_t;

typedef struct {
	dbtag_t type;
	uint32_t _pad;
	uint64_t size;
	uint64_t left, right;
	uint64_t key, value;
} dbnode_t;

#if 0
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
	dbcollection_t;
	dbcache_t;
	dbnode_t;
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
				volatile uint64_t list;
				volatile uint64_t cache;
				struct {
					uint64_t size;
					uint64_t left, right;
					uint64_t key, value;
				};
			};
		};
		struct {
			uint32_t len;
			uint32_t hash;
			uint8_t sval[];
		};
		struct {
			uint32_t _uupad[3];
			uint8_t uuval[16];
		};
		struct {
			volatile uint32_t refcnt; // used only by pidcache.  _pad for everyone else
			union {
				volatile uint64_t obj;
			};
		};
	};
} dbtype_t;
#endif
#pragma pack()

// vim: ts=4 sts=4 sw=4 expandtab:
#endif

