#ifndef PMEM_H
#define PMEM_H

#include <pongo/stdtypes.h>

#define PAGE_SIZE (4096)
#define SMALLEST_ALLOC (16)

#define MEMBLOCK_HEAD \
	uint8_t signature[16]; \
	uint64_t pg_size; \
	uint64_t pg_count; \
	uint64_t mb_offset; \
	uint64_t mb_size; \
	int32_t mb_lfp; \
	int32_t mb_subpool[8]; \
	uint8_t mb_reserved[128 - (16 + 4*sizeof(uint64_t) + 9*sizeof(int32_t))];

struct _memblock_head {
	MEMBLOCK_HEAD
};

typedef struct _memblock {
	MEMBLOCK_HEAD
	uint8_t _pad[PAGE_SIZE - sizeof(struct _memblock_head)];
} memblock_t;

#define PM_FLAG_MASK	0xF
#define PM_CONTINUATION	0x1
#define PM_SUBPAGEFULL	0x2
#define PM_GC_MARK	0x4
#define PM_RESERVED	0x8

extern void *palloc(memblock_t *base, unsigned size);
extern void pfree(memblock_t *base, void *addr);
extern int psize(memblock_t *base, void *addr);

extern void pmem_gc_mark(memblock_t *base);
extern void pmem_gc_keep(memblock_t *base, void *addr);
typedef void (*gcfreecb_t)(void *user, void *addr);
extern void pmem_gc_free(memblock_t *base, gcfreecb_t gc_free_cb, void *user);

typedef void (*pmemcb_t)(void *addr, int size, void *data);
extern void pmem_foreach(memblock_t *base, pmemcb_t func, void *data);
extern void pmem_info(memblock_t *base);

#endif
