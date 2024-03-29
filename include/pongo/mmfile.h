#ifndef MMFILE_H
#define MMFILE_H

#include <stdlib.h>
#ifdef WIN32
#include <windows.h>
#endif
#include <pongo/stdtypes.h>
#include <pongo/log.h>
#include <assert.h>

/*
 * Interface for memory mapped file
 *
 * When a file is initially mapped, the entire contents of the file is mapped
 * into one region.  As the file grows (with mm_resize), additional  chunks
 * are mapped in whereever mmap chooses.
 *
 * The mmap_t structure holds the pointer to each mmap'd chunk and its
 * offset (from the start of the file) and size.
 *
 * Initially, there is a 1:1 correspondence between mmap regions and
 * memblock_t's used by pmem and dbmem.  However, if the program exits and
 * restarts, the entire contents of the file will be in one mapping
 * which will contain several mmap regions.
 */

struct mmtlb  {
	uint64_t offset;
	void *ptr;
};

typedef struct _mmap {
	void *ptr;
	uint64_t offset, size;
#ifdef WIN32
	HANDLE handle;
#endif
} mmap_t;

typedef struct _mmfile {
	const char *filename;
#ifdef WIN32
	HANDLE fd;
#else
	int fd;
#endif
	int nmap;
	mmap_t *map;
	mmap_t *map_offset;
	uint64_t size;
	struct mmtlb tlbofs[256], tlbptr[256];
} mmfile_t;
#define MLCK_RD		0x0001		// Reader lock
#define MLCK_WR		0x0002		// Writer lock
#define MLCK_UN		0x0004		// Unlock
#define MLCK_TRY	0x0008		// Trylock (don't block)
#define MLCK_INTR	0x0010		// Interruptible

int mm_open(mmfile_t *mm, const char *filename, int initsize);
int mm_close(mmfile_t *mm);
int mm_sync(mmfile_t *mm);
int mm_resize(mmfile_t *mm, uint64_t newsize);
int mm_lock(mmfile_t *mm, uint32_t flags, uint64_t offset, uint64_t len);
uint64_t mm_size(mmfile_t *mm);

/*
 * Given a pointer in a mmap region, return the mapping to which it belongs
 */
static inline mmap_t *__mm_ptr(mmfile_t *mm, void *ptr)
{
	int imin, imid, imax;
	mmap_t *map;
	if (!ptr) return 0;
	
	imin = 0;
	imax = mm->nmap-1;
	while(imax >= imin) {
		imid = imin + (imax-imin)/2;
		map = &mm->map[imid];
		if ((uint8_t*)ptr < (uint8_t*)map->ptr) {
			imax = imid-1;
		} else if ((uint8_t*)ptr >= (uint8_t*)map->ptr + map->size) {
			imin = imid+1;
		} else {
			return map;
		}
	}
	log_error("Invalid pointer: %p", ptr);
	abort();
	return NULL;
}

/*
 * Given an offset in a mmap file, return the mapping to which it belongs
 */
static inline mmap_t *__mm_offset(mmfile_t *mm, uint64_t offset)
{
	int imin, imid, imax;
	mmap_t *map;
	uint64_t nsz;
	
again:
	imin = 0;
	imax = mm->nmap-1;
	while(imax >= imin) {
		imid = imin + (imax-imin)/2;
		map = &mm->map_offset[imid];
		if (offset < map->offset) {
			imax = imid-1;
		} else if (offset >= map->offset + map->size) {
			imin = imid+1;
		} else {
			return map;
		}
	}

	// Check if the file has been resized
	nsz = mm_size(mm);
	if (offset < nsz) {
		// FIXME
		mm_resize(mm, nsz);
		goto again;
	}
	log_error("Invalid offset: %" PRIx64, offset);
	abort();
	return NULL;
}

// Translation lookaside buffer (for speeding up __offset and __ptr)
static inline uint8_t __key(uint64_t k)
{
	k ^= (k>>32);
	k ^= (k>>16);
	k ^= (k>>8);
	return k;
}

static inline uint64_t __tlboffset(mmfile_t *mm, void *ptr)
{
	struct mmtlb *t = &mm->tlbofs[__key((uint64_t)ptr)];
	return ptr == t->ptr ? t->offset : 0;
}

static inline void * __tlbptr(mmfile_t *mm, uint64_t offset)
{
	struct mmtlb *t = &mm->tlbptr[__key(offset)];
	return offset == t->offset ? t->ptr : NULL;
}

static inline void __tlbset(mmfile_t *mm, uint64_t offset, void *ptr)
{
	struct mmtlb *t;
       	t = &mm->tlbptr[__key(offset)];
	t->offset = offset; t->ptr = ptr;
	mm->tlbofs[__key((uint64_t)ptr)] = *t;
}

// Given a pointer in a mmap file, return it's 64-bit offset
static inline uint64_t __offset(mmfile_t *mm, void *ptr)
{
	mmap_t *m;
	uint64_t offset=0;

	//offset = __tlboffset(mm, ptr);
	if (!offset) {
	       	m = __mm_ptr(mm, ptr);
		offset = m->offset + ((uint8_t*)ptr - (uint8_t*)m->ptr);
		__tlbset(mm, offset, ptr);
	}
	return offset;
}

// Given an offset in a mmap file, return its pointer
static inline void *__ptr(mmfile_t *mm, uint64_t offset)
{
	mmap_t *m;
	void *ptr = NULL;

	if (!offset) return NULL;
	//ptr = __tlbptr(mm, offset);
	if (!ptr) {
		m = __mm_offset(mm, offset);
		ptr = m->ptr + (offset - m->offset);
		__tlbset(mm, offset, ptr);
	}
	return ptr;
}

#endif
