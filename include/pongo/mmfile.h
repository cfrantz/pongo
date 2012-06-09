#ifndef MMFILE_H
#define MMFILE_H

#ifdef WIN32
#include <windows.h>
#endif
#include <pongo/stdtypes.h>
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
	uint64_t size;
} mmfile_t;

/*
 * Given a pointer in a mmap region, return the mapping to which it belongs
 */
static inline int __ptr_map(mmfile_t *mm, void *ptr)
{
	int i;
	mmap_t *map;
	if (!ptr) return 0;
	for(i=0, map=mm->map; i<mm->nmap; i++, map++) {
		if ((uint8_t*)ptr >= (uint8_t*)map->ptr &&
		    (uint8_t*)ptr <  (uint8_t*)map->ptr + map->size)
			return i;
	}
	return -1;
}

/*
 * Given an offset in a mmap file, return the mapping to which it belongs
 */
static inline int __offset_map(mmfile_t *mm, uint64_t offset)
{
	int i;
	mmap_t *map;
	for(i=0, map=mm->map; i<mm->nmap; i++, map++) {
		if (offset >= map->offset &&
		    offset <  map->offset + map->size)
			return i;
	}
	return -1;
}

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

#endif
