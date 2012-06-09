#include <stdlib.h>
#include <string.h>
#include <windows.h>

#include <pongo/mmfile.h>
#include <pongo/log.h>
#include <pongo/errors.h>

void _addmap(mmfile_t *mm, void *ptr, uint64_t offset, uint64_t size, HANDLE mh)
{
	int n;
	if (mm->nmap % 16 == 0)
		mm->map = realloc(mm->map, sizeof(mmap_t)*(16+mm->nmap));
	assert(mm->map);
	n = mm->nmap++;
	mm->map[n].ptr = ptr;
	mm->map[n].offset = offset;
	mm->map[n].size = size;
	mm->map[n].handle = mh;
}

uint64_t mm_size(mmfile_t *mm)
{
	BY_HANDLE_FILE_INFORMATION fi;
	uint64_t size;
	GetFileInformationByHandle(mm->fd, &fi);
	size = fi.nFileSizeHigh;
	size <<= 32;
	size |= fi.nFileSizeLow;
	return size;
}


#if 0
static int ftruncate(HANDLE h, uint64_t size)
{
	LARGE_INTEGER x;
	BY_HANDLE_FILE_INFORMATION fi;
	uint64_t csize;
	const int chunk = 1024*1024;
	uint32_t written;
	void *zero = malloc(chunk);

	memset(zero, 0, chunk);
	GetFileInformationByHandle(h, &fi);
	csize = fi.nFileSizeHigh;
	csize <<= 32;
	csize |= fi.nFileSizeLow;

	while(csize < size) {
		if (!WriteFile(h, zero, chunk, &written, NULL)) {
			log_error("Error extending file");
			break;
		}
		csize += written;
	}
	free(zero);
	return 0;
}
#else
static int ftruncate(HANDLE h, uint64_t size)
{
	LARGE_INTEGER x;
	x.QuadPart = size;
	if (!SetFilePointerEx(h, x, NULL, FILE_BEGIN)) {
		log_error("Can't SetFilePointer: %d", strerror(errno));
		return -1;
	}
	if (!SetEndOfFile(h)) {
		log_error("Can't SetEndOfFile: %d", strerror(errno));
		return -1;
	}
	return 0;
}
#endif


int mm_open(mmfile_t *mm, const char *filename, int initsize)
{
	uint64_t size;
	void *ptr;
	HANDLE mh;

	mm->fd = CreateFile(filename,
			GENERIC_READ | GENERIC_WRITE,
			FILE_SHARE_READ | FILE_SHARE_WRITE,
			NULL,
			OPEN_ALWAYS,
			FILE_ATTRIBUTE_NORMAL,
			NULL);

	if (mm->fd < 0) {
		log_error("Can't open %s: %s\n", filename, strerror(errno));
		return MERR_OPEN;
	}
	size = mm_size(mm);

	if (size == 0) {
		if (initsize == 0) initsize = 16*1024*1024;
		if (ftruncate(mm->fd, initsize) < 0) {
			log_error("Can't extend file %s to %d bytes: %s\n", mm->filename, initsize, strerror(errno));
			return MERR_SIZE;
		}
		size = mm_size(mm);
	}

	mh = CreateFileMapping(mm->fd, NULL, PAGE_READWRITE, 0, 0, NULL);
	ptr = MapViewOfFile(mh,
			FILE_MAP_READ|FILE_MAP_WRITE,
			0, 0,
			size);

	if (ptr == NULL) {
		log_error("Can't map %s: %s\n", filename, strerror(errno));
		close(mm->fd);
		return MERR_MAP;
	}

	mm->filename = strdup(filename);
	_addmap(mm, ptr, 0, size, mh);
	mm->size = size;
	return 0;
}

int mm_close(mmfile_t *mm)
{
	int i;
	for(i=0; i<mm->nmap; i++) {
		UnmapViewOfFile(mm->map[i].handle);
		CloseHandle(mm->map[i].handle);
	}
	CloseHandle(mm->fd);
	return 0;
}

int mm_sync(mmfile_t *mm)
{
	int i;
	int rc;
	int ret = 0;
	for(i=0; i<mm->nmap; i++) {
		rc = FlushViewOfFile(mm->map[i].ptr, mm->map[i].size);
		if (!rc) {
			log_debug("Could not flush memory segment %p", mm->map[i].ptr);
			ret = -1;
		}
	}
	rc = FlushFileBuffers(mm->fd);
	if (!rc) {
		log_debug("Could not flush file buffers: %s", mm->filename);
		ret = -1;
	}
	return ret;
}


int mm_resize(mmfile_t *mm, uint64_t newsize)
{
	void *ptr;
	uint64_t chunksz;
	uint64_t chunkofs;
	HANDLE mh;
	int n;

	mm->size = mm_size(mm);
	if (mm->size < newsize) {
		if (ftruncate(mm->fd, newsize) < 0) {
			log_error("Can't extend to new size");
			return MERR_MAP;
		}
	} else {
		newsize = mm->size;
	}
	n = mm->nmap - 1;
	chunkofs = mm->map[n].offset + mm->map[n].size;
	chunksz = newsize - chunkofs;

	mh = CreateFileMapping(mm->fd, NULL, PAGE_READWRITE, 
			(uint32_t)(newsize>>32), (uint32_t)(newsize), NULL);

	ptr = MapViewOfFile(mh,
		FILE_MAP_READ|FILE_MAP_WRITE,
		(uint32_t)(chunkofs>>32), (uint32_t)chunkofs,
		chunksz);

	if (!ptr) {
		log_error("Can't map %s: %s\n", mm->filename, strerror(errno));
		close(mm->fd);
		return MERR_MAP;
	}
	_addmap(mm, ptr, chunkofs, chunksz, mh);
	mm->size = newsize;
	return 0;
}

int mm_lock(mmfile_t *mm, uint32_t flags, uint64_t offset, uint64_t len)
{
	int ret;
	uint32_t lflags = (flags & MLCK_TRY) ? LOCKFILE_FAIL_IMMEDIATELY : 0;
	OVERLAPPED ovr = {0, 0, 0, 0, NULL};
	uint32_t llen, hlen;

	if (flags & MLCK_RD) {
		lflags |= 0;
	} else if (flags & MLCK_WR) {
		lflags |= LOCKFILE_EXCLUSIVE_LOCK;
	} else if (flags & MLCK_UN) {
		lflags |= 0;
	} else {
		// Unknown lock flags
		return -1;
	}
	ovr.Offset = (uint32_t)offset;
	ovr.OffsetHigh = (uint32_t)(offset>>32);
	llen = (uint32_t)len;
	hlen = (uint32_t)(len>>32);

	// Always unlock... This is so dbfile_addsize can "upgrade" its
	// shared lock to an exclusive lock
	ret = UnlockFileEx(mm->fd, 0, llen, hlen, &ovr);
	if (!(flags & MLCK_UN)) {
		ret = LockFileEx(mm->fd, lflags, 0, llen, hlen, &ovr);
	}
	ret = -!ret;
	return ret;
}
