#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <errno.h>

#include <pongo/mmfile.h>
#include <pongo/log.h>
#include <pongo/errors.h>

//#define MMAP_SUGGEST (void*)0x10000000
#define MMAP_SUGGEST NULL

void _addmap(mmfile_t *mm, void *ptr, uint64_t offset, uint64_t size)
{
	int n;
	if (mm->nmap % 16 == 0)
		mm->map = realloc(mm->map, sizeof(mmap_t)*(16+mm->nmap));
	assert(mm->map);
	n = mm->nmap++;
	mm->map[n].ptr = ptr;
	mm->map[n].offset = offset;
	mm->map[n].size = size;
}

uint64_t mm_size(mmfile_t *mm)
{
	struct stat file;
	fstat(mm->fd, &file);
	return file.st_size;
}

int mm_open(mmfile_t *mm, const char *filename, int initsize)
{
	int fd;
	struct stat file;
	void *ptr;

	fd = open(filename, O_RDWR | O_CREAT, 0664);
	if (fd < 0) {
		log_error("Can't open %s: %s\n", filename, strerror(errno));
		return MERR_OPEN;
	}
	fstat(fd, &file);

	if (file.st_size == 0) {
		if (initsize == 0) initsize = 16*1024*1024;
		if (ftruncate(fd, initsize) < 0) {
			log_error("Can't extend file %s to %d bytes: %s\n", mm->filename, initsize, strerror(errno));
			return MERR_SIZE;
		}
		fstat(fd, &file);
	}

	ptr = mmap(MMAP_SUGGEST, file.st_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	if (ptr == MAP_FAILED) {
		log_error("Can't map %s: %s\n", filename, strerror(errno));
		close(fd);
		return MERR_MAP;
	}

	mm->filename = strdup(filename);
	mm->fd = fd;
	_addmap(mm, ptr, 0, file.st_size);
	mm->size = file.st_size;
	return 0;
}

int mm_close(mmfile_t *mm)
{
	int i;
	for(i=0; i<mm->nmap; i++)
		munmap(mm->map[i].ptr, mm->map[i].size);
	close(mm->fd);
	return 0;
}

int mm_sync(mmfile_t *mm)
{
	return fdatasync(mm->fd);
}

int mm_resize(mmfile_t *mm, uint64_t newsize)
{
	void *ptr;
	uint64_t chunksz;
	uint64_t chunkofs;
	int n;

	if (mm->size < newsize) {
		if (ftruncate(mm->fd, newsize) < 0) {
			log_error("Can't extend file %s to %d bytes: %s\n", mm->filename, newsize, strerror(errno));
			return MERR_SIZE;
		}

		n = mm->nmap - 1;
		chunkofs = mm->map[n].offset + mm->map[n].size;
		chunksz = newsize - chunkofs;
		ptr = mmap(MMAP_SUGGEST, chunksz, PROT_READ|PROT_WRITE, MAP_SHARED, mm->fd, chunkofs);
		if (ptr == MAP_FAILED) {
			log_error("Can't map %s: %s\n", mm->filename, strerror(errno));
			close(mm->fd);
			return MERR_MAP;
		}
		_addmap(mm, ptr, chunkofs, chunksz);
		mm->size = newsize;
	}
	return 0;
}

int mm_lock(mmfile_t *mm, uint32_t flags, uint64_t offset, uint64_t len)
{
	int ret;
	struct flock fl;
	int cmd = (flags & MLCK_TRY) ? F_SETLK : F_SETLKW;

	if (flags & MLCK_RD) {
		fl.l_type = F_RDLCK;
	} else if (flags & MLCK_WR) {
		fl.l_type = F_WRLCK;
	} else if (flags & MLCK_UN) {
		fl.l_type = F_UNLCK;
	} else {
		// Unknown lock flags
		return -1;
	}

	fl.l_whence = SEEK_SET;
	fl.l_start = offset;
	fl.l_len = len;


	do {
		ret = fcntl(mm->fd, cmd, &fl);
	} while (ret < 0 && errno == EINTR && !(flags & MLCK_INTR));
	return ret;
}
