#ifndef PMEM_H
#define PMEM_H

#include <pongo/stdtypes.h>
#include <pongo/mmfile.h>

#define offsetof(st, el)  ((uintptr_t)(&((st*)0)->el))
#define PMEM_LOCKFREE 1
#define PAGE_SIZE (4096)
#define SMALLEST_ALLOC (16)
#define NR_SZCLS 16

typedef struct _pdescriptor {
	union {
		struct {
			uint32_t s_ofs, e_ofs;
		};
		uint64_t all;
	};
} pdescr_t;
		
#define SIG_MEMPOOL 0x204c4f4f504d454d
typedef struct _mempool {
	uint64_t signature;
	uint64_t next;
	uint32_t size;
	uint32_t largest_free, total_free;
	uint8_t __pad[64-(2*sizeof(uint64_t)+3*sizeof(uint32_t))];
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
	uint32_t
		type:	1,
		alloc:	1,
		gc:	1,
		suggest:1,
		_resv:  12,
		next: 16;
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

#define SIG_SUPERBLK 0x04b4c425245505553ULL
typedef struct _superblock {
	uint64_t signature;
	uint64_t next;
	volatile bdescr_t desc;
	uint32_t size, total;
	uint32_t _xxx;
	uint32_t gc :1,
		 suggest: 1,
		_resv: 30;
	uint8_t _pad[64 - (5*sizeof(uint64_t))];
} superblock_t;

typedef struct _mlist {
	uint64_t freelist;
	uint64_t fulllist;
} mlist_t;

typedef struct _procheap {
	int64_t last_used;
	uint64_t id;
	mlist_t szcls[NR_SZCLS];
	uint8_t _pad[64 - (NR_SZCLS*sizeof(mlist_t)+2*sizeof(uint64_t)) % 64];
} procheap_t;

typedef struct _plist {
	int32_t sizes[17];
	uint32_t _pad;
	uint64_t nr_pools;
	uint64_t pool[64];  // the sizeof calculation for allocating plist_t will give
	                    // this many spare slots
} plist_t;

typedef struct _memheap {
	uint64_t nr_procheap;
	uint64_t mempool;
	uint64_t pool;
	uint64_t pool_alloc;
	uint8_t _pad1[64-sizeof(uint64_t)];
	procheap_t procheap[];
} memheap_t;


extern void *(*pmem_more_memory)(mmfile_t *mm, uint32_t *size);
extern mempool_t *pmem_pool_init(void *addr, uint32_t size);
extern void *pmem_pool_alloc(mempool_t *pool, uint32_t size);
extern int pmem_pool_free(void *addr);
extern void pmem_pool_print(mempool_t *pool);

extern superblock_t *pmem_sb_init(void *mem, uint32_t blksz, uint32_t count);
extern void *pmem_sb_alloc(superblock_t *sb);
extern void pmem_sb_free(superblock_t *sb, void *addr);

extern void *pmem_alloc(mmfile_t *mm, memheap_t *heap, uint32_t sz);
extern void pmem_retire(mmfile_t *mm, memheap_t *heap, int ph);
extern void pmem_gc_mark(mmfile_t *mm, memheap_t *heap, int suggest);
extern void pmem_relist_pools(mmfile_t *mm, memheap_t *heap);

typedef void (*gcfreecb_t)(void *user, void *addr);
extern void pmem_gc_free(mmfile_t *mm, memheap_t *heap, int fast, gcfreecb_t cb, void *user);

extern void pmem_print_mem(mmfile_t *mm, memheap_t *heap);

static inline void pmem_gc_suggest(void *addr, int x)
{
    memblock_t *mb;
    superblock_t *sb;

    if (!addr) return;
    mb = (memblock_t*)addr - 1;
    if (mb->type == 1) {
	assert(mb->alloc);
        mb->suggest = 1;
	mb->_resv = x;
        sb = (superblock_t*)((uint8_t*)mb - mb->sbofs);
        sb->suggest = 1;
    }
}

static inline void pmem_gc_keep(void *addr)
{
    memblock_t *mb;
    if (!addr) return;
    mb = (memblock_t*)addr - 1;
    assert(mb->alloc);
    mb->gc = 0;
}


#endif
