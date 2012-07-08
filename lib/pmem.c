#include <stdlib.h>
#include <string.h>
#include <pongo/stdtypes.h>
#include <pongo/pmem.h>
#include <pongo/misc.h>
#include <pongo/log.h>

/*
 * A simple memory allocator, based on "Kernel Memory Management" from
 * "The Design and Implementation of the 4.4BSD Operating System".
 *
 * The memory is mapped in from the filesystem in MEMBLOCK_SIZE chunks.
 * The first bit of each chunk (about 0.9% of the total) contains the
 * allocation control structures:
 *
 * There is a memsize map.  Each page can be sub allocated into smaller
 * chunks, in powers of 2 from SMALLEST_ALLOC to PAGE_SIZE. 
 *
 * Allocations of PAGE_SIZE and larger are rounded to the nearest
 * PAGE_SIZE and allocated.
 *
 * First page: signature and pg_size, pg_count
 * Next: pg_count words of page allocation map (memsize)
 * Next: total_size_in_bytes/16 bits of sub-page allocation map
 * Next: Free space up until total_size_in_bytes
 *
 * For a 16 MB chunk in 4k pages, this works out to:
 * Header page
 * 4 pages of page allocation map
 * 32 pages of sub-page allocation map
 * 4059 user pages
 *
 * About 0.9% is spent on allocation accounting.
 */

static inline int bsearch_word(uint32_t word)
{
	uint32_t mask = 0xFFFFFFFFUL;
	int shift = 0;
	int bits = 16;
	int i;

	if (word == mask)
		return -1;

	for(i=0; i<5; i++, bits /= 2) {
		mask >>= bits;
		if ((word & mask) == mask) {
			word >>= bits;
			shift += bits;
		}
	}
	return shift;
}

static int search_vec(uint32_t *vec, int maxbits)
{
	int bit = 0;
	int b;
	while(maxbits) {
		b = bsearch_word(*vec);
		if (b<0) {
			maxbits -= 32;
			bit += 32;
			vec++;
		} else if (b >= maxbits) {
			break;
		} else {
			return bit+b;
		}
	}
	return -1;
}

static inline uint32_t firstpage(memblock_t *base)
{
	uint32_t total = base->pg_size * base->pg_count;
	uint32_t pgpool = base->pg_count;
	uint32_t nbitmap = total / (SMALLEST_ALLOC * 32);
	uint32_t first = 1 + ((pgpool+2*nbitmap)*sizeof(uint32_t) + base->pg_size-1) / base->pg_size;
	return first;
}

static int pagealloc(memblock_t *base, uint32_t size)
{
	int i, pages;
	int start, tpages;
	// The alloc accounting data starts on the first page after the header
	uint32_t *memsize = (uint32_t*)( (uint8_t*)base + base->pg_size );
	// Compute the number of words used for accounting, then
	// figure out the first usable page after the accounting data.
	uint32_t total = base->pg_size * base->pg_count;
	uint32_t pgpool = base->pg_count;
	uint32_t first = firstpage(base);
	int32_t lfp = base->mb_lfp;

	// error if greater than max allowable allocation
	// or if no pages left
	if (size > total/2 || lfp == -1)
		return -1;

	// Adjust size to pages
	pages = (size + base->pg_size-1) / base->pg_size;
		
	// Scan through the page list looking for "pages" free
	// blocks in a row
	start = -1;
	if (lfp == 0) lfp = first;

again:
	tpages = 0;
	for(i=lfp; i<pgpool; i++) {
		if (memsize[i] == 0) {
			if (start == -1) {
				start = i;
				tpages = pages;
			}
			if (--tpages == 0) {
				break;
			}
		} else {
			if (start != -1)
				start = -1;
		}
	}
	// Did we hit the end of the pool, but still need to allocate
	// more pages?
	if (i==pgpool && tpages)
		start = -1;

	// If we didn't find a page, reset last-free-page to
	// first and rescan.
	if (start==-1 && i==pgpool) {
		if (lfp != first) {
			lfp = first;
			goto again;
		}
		// If after the second time through we didn't find a
		// page, set lfp=-1 to indicate this pool is full
		base->mb_lfp = -1;
	}

	if (start != -1) {
		memsize[start] = size;
		for(i=1; i<pages; i++) 
			memsize[start + i] = PM_CONTINUATION;
	}

	return start;
}

static void pagefree(memblock_t *base, uint32_t page)
{
	// The alloc accounting data starts on the first page after the header
	uint32_t *memsize = (uint32_t*)( (uint8_t*)base + base->pg_size );
	uint32_t pgpool = base->pg_count;

	if (memsize[page] == 0) {
		log_error("double free: %p\n", (uint8_t*)base+page*base->pg_size);
		abort();
	}
	// If the pool has been marked full, reset the marker
	if (base->mb_lfp == -1)
		base->mb_lfp = page;
	// Free the page
	memsize[page++] = 0;

	// Free any continuation
	while(memsize[page] == PM_CONTINUATION && page < pgpool)
		memsize[page++] = 0;

}

static void *suballoc(memblock_t *base, uint32_t size)
{
	int i, bits, block;
	// The alloc accounting data starts on the first page after the header
	uint32_t *memsize = (uint32_t*)( (uint8_t*)base + base->pg_size );
	// Compute the number of words used for accounting, then
	// figure out the first usable page after the accounting data.
	uint32_t pgpool = base->pg_count;
	uint32_t first = firstpage(base);
	uint32_t allocwords_page = base->pg_size / (SMALLEST_ALLOC*32);
	uint32_t *bitmap = memsize + pgpool;
	int spi = ntz(size) - 4;


	// Find a page with a suballocation of the requested size
	i = base->mb_subpool[spi];
	if (i<0) return NULL;
	if (i>0 && memsize[i] == size)
		goto found;
	
	i = first;
scan:
	while(i<pgpool) {
		if (memsize[i] == size)
			break;
		i++;
	}
	// If we couldn't find one, pagealloc a new page
	if (i == pgpool) {
		i = pagealloc(base, size);
		if (i == -1) {
			// No pages left, so we can't do this allocation
			// mark the subpool pointer as such
			base->mb_subpool[spi] = -1;
			return NULL;
		}
	}
	// Found a page with our allocation size, so makr the
	// subpool pointer
	base->mb_subpool[spi] = i;

found:
	// Compute how many bits for this suballocation
	// and where the bitmap for this page lives
	bits = base->pg_size / size;

	// Search for a free bit
	block = search_vec(bitmap + i*allocwords_page, bits);
	if (block == -1) {
		// If the block was full, mark it
		// and continue scanning
		memsize[i] |= PM_SUBPAGEFULL;
		goto scan;
	}

	// Mark the bitmap and compute the address of this block
	bitmap[i*allocwords_page + block/32] |= 1UL << (block % 32);
	return (uint8_t*)base + i*base->pg_size + block*size;
}

static void subfree(memblock_t *base, void *addr)
{
	int page, block, offset, size;
	uint32_t mask;
	// The alloc accounting data starts on the first page after the header
	uint32_t *memsize = (uint32_t*)( (uint8_t*)base + base->pg_size );
	// Compute the number of words used for accounting, then
	// figure out the first usable page after the accounting data.
	uint32_t pgpool = base->pg_count;
	uint32_t allocwords_page = base->pg_size / (SMALLEST_ALLOC*32);
	uint32_t *bitmap = memsize + pgpool;
	int spi;

	// Compute which page we're in and get the size
	offset = (uint8_t*)addr - (uint8_t*)base;
	page = offset / base->pg_size;
	size = memsize[page] & ~PM_FLAG_MASK;
	// Compute which sub-block we're in and compute the mb_subpool index
	block = (offset % base->pg_size) / size;
        spi = ntz(size) - 4;

	// Clear the bit in the bitmap
	offset = page * allocwords_page + block/32;
	mask = 1UL << (block % 32);
	if ((bitmap[offset] & mask) == 0)  {
		log_error("double free: %p\n", addr);
		abort();
	}
	bitmap[offset] &= ~mask;

	// Clear the full bit (in case it was set)
	memsize[page] &= ~PM_SUBPAGEFULL;

	// If the subpool was marked as full, mark
	// with the newly freed page
	if (base->mb_subpool[spi] == -1)
		base->mb_subpool[spi] = page;
}

void *palloc(memblock_t *base, unsigned size)
{
	uint8_t *addr = NULL;
	int page;
	if (size < SMALLEST_ALLOC) size = SMALLEST_ALLOC;

	if (size <= base->pg_size/2) {
		size = pow2(size);
		addr = suballoc(base, size);
	} else {
		size = (size + base->pg_size-1) & ~(base->pg_size-1);
		page = pagealloc(base, size);
		if (page != -1)
			addr = (uint8_t*)base + page*base->pg_size;
	}
	return (void*)addr;
}

void pfree(memblock_t *base, void *addr)
{
	int page, size;
	uint32_t *memsize = (uint32_t*)( (uint8_t*)base + base->pg_size );

	if (!addr)
		return;

	page = ((uint8_t*)addr - (uint8_t*)base) / base->pg_size;
	size = memsize[page];

	if (size == 1) {
		log_error("pfree: address %p is in a continuation\n", addr);
		abort();
	} else if (size < base->pg_size) {
		subfree(base, addr);
	} else {
		pagefree(base, page);
	}
}

int psize(memblock_t *base, void *addr)
{
	int page, size;
	uint32_t *memsize = (uint32_t*)( (uint8_t*)base + base->pg_size );

	if (!addr)
		return 0;

	page = ((uint8_t*)addr - (uint8_t*)base) / base->pg_size;
	size = memsize[page] & ~PM_FLAG_MASK;
	return size;
}

void pmem_gc_mark(memblock_t *base)
{
	uint32_t *memsize = (uint32_t*)( (uint8_t*)base + base->pg_size );
	// Compute the number of words used for accounting, then
	// figure out the first usable page after the accounting data.
	uint32_t total = base->pg_size * base->pg_count;
	uint32_t pgpool = base->pg_count;
	uint32_t nbitmap = total / (SMALLEST_ALLOC * 32);
	uint32_t first = firstpage(base);
	uint32_t *bitmap = memsize + pgpool;
	uint32_t *bitmark = memsize + pgpool + nbitmap;
	int i;

	// There are two kinds of marks for the garbage collector
	// 1. A mark in the memsize array for allocation of page_size or larger
	// 2. A bitmark array for suballocations less than page_size
	for(i=first; i<pgpool; i++) {
		if ((memsize[i] & ~PM_FLAG_MASK) >= base->pg_size)
			memsize[i] |= PM_GC_MARK;
	}
	memcpy(bitmark, bitmap, nbitmap*sizeof(uint32_t));
}

void pmem_gc_keep(memblock_t *base, void *addr)
{
	uint32_t *memsize = (uint32_t*)( (uint8_t*)base + base->pg_size );
	// Compute the number of words used for accounting, then
	// figure out the first usable page after the accounting data.
	uint32_t total = base->pg_size * base->pg_count;
	uint32_t pgpool = base->pg_count;
	uint32_t nbitmap = total / (SMALLEST_ALLOC * 32);
	uint32_t allocwords_page = base->pg_size / (SMALLEST_ALLOC*32);
	uint32_t *bitmark = memsize + pgpool + nbitmap;
	uint32_t offset, page, size, block, mask;

	if (!addr)
		return;

	offset = (uint8_t*)addr - (uint8_t*)base;
	page = offset / base->pg_size;
	size = memsize[page];
	if (size == 1) {
		log_error("pmem_gc_keep: address %p is in a continuation\n", addr);
		abort();
	} else if (size >= base->pg_size) {
		memsize[page] &= ~PM_GC_MARK; 
	} else {
		// Clear any flag bits in the size
		size &= ~PM_FLAG_MASK;
		// Compute which sub-block we're in
		block = (offset % base->pg_size) / size;

		// Clear the bit in the bitmap
		offset = page * allocwords_page + block/32;
		mask = 1UL << (block % 32);
		bitmark[offset] &= ~mask;
	}
}

void pmem_gc_free(memblock_t *base, gcfreecb_t gc_free_cb, void *user)
{
	uint32_t *memsize = (uint32_t*)( (uint8_t*)base + base->pg_size );
	// Compute the number of words used for accounting, then
	// figure out the first usable page after the accounting data.
	uint32_t pgpool = base->pg_count;
	uint32_t total = base->pg_size * base->pg_count;
	uint32_t nbitmap = total / (SMALLEST_ALLOC * 32);
	uint32_t first = firstpage(base);
	uint32_t allocwords_page = base->pg_size / (SMALLEST_ALLOC*32);
	uint32_t *bitmap = memsize + pgpool;
	uint32_t *bitmark = memsize + pgpool + nbitmap;
	int i, j, page, spi, size;
	uint32_t a;
	void *addr;

	// First clear the PM_GC_MARK bits in the memsize array
	for(i=first; i<pgpool; i++) {
		if (memsize[i] & PM_GC_MARK) {
			// If the lfp is -1 (no more pages), set it to
			// the index, because we're freeing it.
			if (base->mb_lfp == -1)
				base->mb_lfp = i;
			if (gc_free_cb) {
				addr = (uint8_t*)base + base->pg_size * i;
				gc_free_cb(user, addr);
			}
			memsize[i] = 0;
			while(memsize[i+1] == PM_CONTINUATION)
				memsize[++i] = 0;
		}
	}
	// Now clear all the bits in the bitmask array that have
	// their corresponding bit set in the bitmark array
	a = 0;
	for(i=first*allocwords_page; i<nbitmap; i++) {
		page = i / allocwords_page;
		if (gc_free_cb && bitmark[i]) {
			size = memsize[page] & ~PM_FLAG_MASK;
			for(j=0; j<32; j++) {
				if (bitmark[i] & (1UL<<j)) {
					addr = (uint8_t*)base + (base->pg_size*page +
						       size*((i%allocwords_page)*32+j));
					gc_free_cb(user, addr);
				}
			}
		}
		//if (bitmark[i]) log_bare("%d %04x %08x", i, page, bitmark[i]);
		bitmap[i] &= ~bitmark[i];
		a |= bitmap[i];
		if (bitmark[i]) {
			// If we had frees in this suballoction, clear the
			// subpagefull flag in the memsize array
			memsize[page] &= ~PM_SUBPAGEFULL;

			// If the mb_subpool is -1 set it to this page, since
			// there are free chunks
			spi = ntz(memsize[page]) - 4;
			if (base->mb_subpool[spi] == -1)
				base->mb_subpool[spi] = page;
		}
		if ((i+1)%allocwords_page==0) {
			// If we've scanned a whole page's worth of bitmap
			// bits and none were set, we might be able to
			// free the page back to the page allocator.
			if (a == 0) {
				if (memsize[page] >= SMALLEST_ALLOC && memsize[page] < base->pg_size) {
					spi = ntz(memsize[page]) - 4;
					if (base->mb_subpool[spi] == -1)
						base->mb_subpool[spi] = 0;
					if (base->mb_lfp == -1)
						base->mb_lfp = i;
					memsize[page] = 0;
				}
			} else {
				a = 0;
			}
		}
	}
}

void pmem_foreach(memblock_t *base, pmemcb_t func, void *data)
{
	int i, j, size, bits;
	uint8_t *addr;
	// The alloc accounting data starts on the first page after the header
	uint32_t *memsize = (uint32_t*)( (uint8_t*)base + base->pg_size );
	// Compute the number of words used for accounting, then
	// figure out the first usable page after the accounting data.
	uint32_t pgpool = base->pg_count;
	uint32_t first = firstpage(base);
	uint32_t allocwords_page = base->pg_size / (SMALLEST_ALLOC*32);
	uint32_t *bitmap = memsize + pgpool;
	
	// For each allocated chunk, call "func" with
	// the address, size and data.
	for(i=first; i<pgpool; i++) {
		size = memsize[i] & ~PM_FLAG_MASK;
		if (size >= base->pg_size) {
			addr = (uint8_t*)base + i*base->pg_size;
			func(addr, size, data);
		} else if (size > 0) {
			bits = base->pg_size / size;
			for(j=0; j<bits; j++) {
				if (bitmap[i*allocwords_page + j/32] & (1UL << (j%32))) {
					addr = (uint8_t*)base + i*base->pg_size + j*size;
					func(addr, size, data);
				}
			}
		}
	}
}

void pmem_info(memblock_t *base)
{
	uint32_t *memsize = (uint32_t*)( (uint8_t*)base + base->pg_size );
	uint32_t pgpool = base->pg_count;
	uint32_t first = firstpage(base);
	uint32_t allocwords_page = base->pg_size / (SMALLEST_ALLOC*32);
	uint32_t *bitmap = memsize + pgpool;
	uint32_t sz, flags;
	int i, j;
	int a, bits;
	uint32_t fpg = 0;

	log_bare("Info for memblock %p: offset=%016llx size=%08llx", base, base->mb_offset, base->mb_size);
	log_bare("Pages 0 to 0x%04x contain memory allocation structs", first);
	for(i=first; i<pgpool; i++) {
		sz = memsize[i];
		flags = sz & PM_FLAG_MASK;
		sz &= ~PM_FLAG_MASK;
		if (flags & PM_CONTINUATION) continue;
		if (sz==0) {
			fpg++;
			continue;
		}
		if (sz<4096) {
			bits = base->pg_size / sz;
			a = 0;
			for(j=0; j<bits; j++) {
				if (bitmap[i*allocwords_page + j/32] & (1UL << (j%32)))
					a++;
			}
			log_bare("page 0x%04x: size=%04x flags=%x (%d/%d allocated)", i, sz, flags, a, bits);
		} else {
			log_bare("page 0x%04x: size=%04x flags=%x", i, sz, flags);
		}
	}
	log_bare("Free pages: %d", fpg);
	log_bare("mb_lfp=%04x mb_subpool=%04x %04x %04x %04x %04x %04x %04x %04x",
			base->mb_lfp,
			base->mb_subpool[0], base->mb_subpool[1],
			base->mb_subpool[2], base->mb_subpool[3],
			base->mb_subpool[4], base->mb_subpool[5],
			base->mb_subpool[6], base->mb_subpool[7]);
	log_bare("");
}
