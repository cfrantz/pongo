#ifndef PMEM_H
#define PMEM_H

#include <pongo/stdtypes.h>
#include <pongo/mmfile.h>

#define offsetof(st, el)  ((uintptr_t)(&((st*)0)->el))
#define PMEM_LOCKFREE 1
#define PAGE_SIZE (4096)
#define SMALLEST_ALLOC (16)
#define NR_SZCLS 6

typedef struct _pdescriptor {
	union {
		struct {
			uint32_t s_ofs, e_ofs;
		};
		uint64_t all;
	};
} pdescr_t;
		
typedef struct _mempool {
	uint64_t next;
	uint32_t size;
	uint8_t __pad[64-(sizeof(uint64_t)+sizeof(uint32_t))];
	volatile pdescr_t desc[1];
} mempool_t;

typedef struct _poolblock {
	uint64_t next;
	uint32_t size;
	uint32_t
		type:	1,
		alloc:	1,
		gc:	1,
		pool:	29;
} poolblock_t;

typedef struct _memblock {
	uint32_t sbofs;
	uint16_t next;
	uint16_t
		type:	1,
		alloc:	1,
		gc:	1,
		_resv:  13;
} memblock_t;

typedef union _bdescr { 
		struct {
			uint16_t free;
			uint16_t count;
			uint16_t size;
			uint16_t tag;
		};
		uint64_t all;
} bdescr_t;

typedef struct _superblock {
	uint64_t next;
	volatile bdescr_t desc;
	uint32_t size, total;
	uint32_t _xxx;
	uint32_t gc :1,
		_resv: 31;
	uint8_t _pad[64 - (4*sizeof(uint64_t))];
} superblock_t;

typedef struct _mlist {
	uint64_t freelist;
	uint64_t fulllist;
} mlist_t;

typedef struct _procheap {
	int64_t last_used;
	uint64_t id;
	mlist_t szcls[NR_SZCLS];
	uint8_t _pad[128 - (6*sizeof(mlist_t)+2*sizeof(uint64_t))];
} procheap_t;

typedef struct _memheap {
	uint64_t nr_procheap;
	mlist_t pool;
	uint8_t _pad0[64-2*sizeof(uint64_t)];
	uint64_t pool_alloc;
	uint8_t _pad1[64-sizeof(uint64_t)];
	procheap_t procheap[];
} memheap_t;


extern void *(*pmem_more_memory)(mmfile_t *mm, uint32_t *size);
extern mempool_t *pmem_pool_init(void *addr, uint32_t size);
extern void *pmem_pool_alloc(mempool_t *pool, uint32_t size);
extern int pmem_pool_free(void *addr);
extern void pmem_pool_print(mempool_t *pool);

extern superblock_t *pmem_sb_init(mempool_t *pool, uint32_t blksz, uint32_t count);
extern void *pmem_sb_alloc(superblock_t *sb);
extern void pmem_sb_free(superblock_t *sb, void *addr);

extern void *pmem_alloc(mmfile_t *mm, memheap_t *heap, uint32_t sz);
extern void pmem_gc_mark(mmfile_t *mm, memheap_t *heap);
extern void pmem_gc_suggest(void *addr);
extern void pmem_gc_free(mmfile_t *mm, memheap_t *heap, int fast);

#endif
