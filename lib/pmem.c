#include <stdlib.h>
#include <string.h>
#include <pongo/stdtypes.h>
#include <pongo/pmem.h>
#include <pongo/atomic.h>
#include <pongo/misc.h>
#include <pongo/log.h>

/*
 * A simple memory allocator, based on "Kernel Memory Management" from
 * "The Design and Implementation of the 4.4BSD Operating System".
 *
 * The memory is mapped in from the filesystem in MEMBLOCK_SIZE chunks.
 * The first bit of each chunk (about 1.7% of the total) contains the
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
 * 32 pages of sub-page allocation GC marks
 * 4027 user pages
 *
 * About 1.7% is spent on allocation accounting.
 */

static inline int bsearch_word(uint64_t word)
{
	uint64_t mask = 0xFFFFFFFFFFFFFFFFULL;
	int shift = 0;
	int bits = sizeof(mask)*8/2;
	int i;

	if (word == mask)
		return -1;

	for(i=0; i<6; i++, bits /= 2) {
		mask >>= bits;
		if ((word & mask) == mask) {
			word >>= bits;
			shift += bits;
		}
	}
	return shift;
}

static int search_vec(uint64_t *bitvec, int maxbits)
{
	uint64_t oldval, newval, *vec;
	int bit = 0;
	int b;
again:
	vec = bitvec;
	while(maxbits) {
		oldval = *vec;
		b = bsearch_word(oldval);
		if (b<0) {
			maxbits -= 64;
			bit += 64;
			vec++;
		} else if (b >= maxbits) {
			break;
		} else {
			newval = oldval | 1ULL << b;
			if (cmpxchg64(vec, oldval, newval))
				return bit+b;
			goto again;
		}
	}
	return -1;
}

static int clear_bit(uint64_t *bitvec, int bit)
{
	uint64_t oldval, newval, mask;
	int offset;
	// Clear the bit in the bitmap
	offset = bit/64;
	mask = 1ULL << (bit % 64);
	
	do {
		oldval = bitvec[offset];
		if ((oldval & mask) == 0)  {
			// bit wasn't set
			return -1;
		}
		newval = oldval & ~mask;
	} while(!cmpxchg64(&bitvec[offset], oldval, newval));
	return 0;
}

static inline uint32_t firstpage(memblock_t *base)
{
	uint32_t total = base->pg_size * base->pg_count;
	uint32_t pgpool = base->pg_count;
	uint32_t nbitmap = total / (SMALLEST_ALLOC * 64);
	uint32_t first = 1 + ((pgpool+2*nbitmap)*sizeof(uint64_t) + base->pg_size-1) / base->pg_size;
	return first;
}

static inline int
mark_continuation(volatile uint32_t *memsize, int pages)
{
	int i;
	uint32_t oldval, newval;
	for(i=1; i<pages; i++) {
		oldval = memsize[i];
		if (oldval & ~PM_GUARD)
			break;
		newval = PM_GUARD_INC(oldval) | PM_CONTINUATION;
		if (!cmpxchg32(&memsize[i], oldval, newval))
			break;
	}
	if (i < pages) {
		// Clear the continuation bit on any pages
		// we successfully claimed
		while(--i) {
			memsize[i] &= ~PM_CONTINUATION;
		}
	}
	return (i==pages);
}

static int pagealloc(memblock_t *base, uint32_t size)
{
	int i, n, pages;
	int start;
	// The alloc accounting data starts on the first page after the header
	volatile uint32_t *memsize = (uint32_t*)( (uint8_t*)base + base->pg_size );
	uint32_t oldval, newval;
	// Compute the number of words used for accounting, then
	// figure out the first usable page after the accounting data.
	uint32_t total = base->pg_size * base->pg_count;
	uint32_t pgpool;
	uint32_t first = firstpage(base);
	int32_t lfp;

	// error if greater than max allowable allocation
	if (size > total/2)
		return -1;

	// Adjust size to pages
	pages = (size + base->pg_size-1) / base->pg_size;
	// error if no pages left
	lfp = base->mb_lfp;
	if (lfp == -1)
		return -1;

	// Scan through the page list looking for "pages" free
	// blocks in a row
	start = -1;
	if (lfp == 0) lfp = first;

again:
	i = lfp;
	//i = first; //testing
	pgpool = base->pg_count - pages;
	while (i < pgpool) {
		oldval = memsize[i];
		if ((oldval & ~PM_GUARD) == 0) {
			// Tentatively reserve the pages
			newval = PM_GUARD_INC(oldval) | size;
			if (!cmpxchg32(&memsize[i], oldval, newval))
				continue;

			// Check if we can get the entire requested size
			if (mark_continuation(memsize+i, pages)) {
				start = i;
				break;
			} else {
				// Nope, so undo the reservation
				do {
					oldval = memsize[i];
					newval = PM_GUARD_INC(oldval) & PM_GUARD;
				} while(!cmpxchg32(&memsize[i], oldval, newval));
				oldval = newval;
			}
		}
		n = PM_SIZE(oldval) / base->pg_size;
		if (n == 0) n=1;
		i += n;
	}
	// If we didn't find a page, reset last-free-page to
	// first and rescan.
	if (start==-1) {
		if (lfp != first) {
			lfp = first;
			goto again;
		}
		// If after the second time through we didn't find a
		// page, set lfp=-1 to indicate this pool is full
		base->mb_lfp = -1;
	} else {
		i = start+pages;
		if (i < base->pg_count && PM_SIZE(memsize[i]) == 0)
			base->mb_lfp = i;
	}
	return start;
}

static void pagefree(memblock_t *base, uint32_t page)
{
	// The alloc accounting data starts on the first page after the header
	volatile uint32_t *memsize = (uint32_t*)( (uint8_t*)base + base->pg_size );
	uint32_t lfp = page;
	uint32_t oldval, newval, cval;
	int i, n;

	oldval = memsize[page];
	if (PM_SIZE(oldval) == 0) {
		log_error("double free: %p\n", (uint8_t*)base+page*base->pg_size);
		abort();
	}

	// Free any continuation
	n = page + PM_SIZE(oldval) / base->pg_size;
	for(i=page+1; i<n; i++) {
		cval = memsize[i];
		if ((cval & ~PM_GUARD) != PM_CONTINUATION) {
			log_error("page %d should be a continuation, but is 0x%x instead", i, memsize[i]);
		} else {
			memsize[i] = PM_GUARD_INC(cval) & ~PM_CONTINUATION;
		}
	}

	// Free the page
	newval = PM_GUARD_INC(oldval) & PM_GUARD;
	memsize[page] = newval;

	// If the pool has been marked full, reset the marker
	if (base->mb_lfp == -1)
		base->mb_lfp = lfp;

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
	uint32_t allocwords_page = base->pg_size / (SMALLEST_ALLOC*64);
	uint64_t *bitmap = (uint64_t*)(memsize + pgpool);
	uint32_t oldval, newval;
	int spi = ntz(size) - 4;
	int end;

	// Find a page with a suballocation of the requested size
	i = end = base->mb_subpool[spi];
	if (i<0) return NULL;
	if (i>0 && PM_SIZE(memsize[i]) == size)
		goto found;
	
	end = i = first;
scan:
	while(1) {
		oldval = memsize[i];
		if (!(oldval & PM_SUBPAGEFULL) && PM_SIZE(oldval) == size)
			break;
		i++;
		if (i == pgpool) i = first;
		if (i == end) break;
	}
	// If we couldn't find one, pagealloc a new page
	if (i == end) {
		i = pagealloc(base, size);
		if (i == -1) {
			// No pages left, so we can't do this allocation
			// mark the subpool pointer as such
			base->mb_subpool[spi] = -1;
			return NULL;
		}
	}
	// Found a page with our allocation size, so mark the
	// subpool pointer
	base->mb_subpool[spi] = i;

found:
	// Reload memsize[i] and check.  The size check should
	// only fail if we're in here and the GC detects no allocations
	// in this page and frees the page 
	oldval = memsize[i];
	if (PM_SIZE(oldval) != size)
		goto scan;

	// Compute how many bits for this suballocation
	// and where the bitmap for this page lives
	bits = base->pg_size / size;

	// Search and allocat a free bit
	block = search_vec(bitmap + i*allocwords_page, bits);
	if (block == -1) {
		// If the block was full, mark it
		// and continue scanning.  Don't care if this
		// cmpxchg fails
		newval = PM_GUARD_INC(oldval) | PM_SUBPAGEFULL;
		cmpxchg32(&memsize[i], oldval, newval);
		goto scan;
	}

	// Increment the Guard.  This will interfere with the GC if
	// it is trying to free this page.
	while(!cmpxchg32(&memsize[i], oldval, PM_GUARD_INC(oldval))) {
		// On the other hand, if something changed, check the
		// size again and retry the cmpxchg
		oldval = memsize[i];
		if (PM_SIZE(oldval) != size)
			goto scan;
	}
	return (uint8_t*)base + i*base->pg_size + block*size;
}

static void subfree(memblock_t *base, void *addr)
{
	int page, block, offset, size;
	// The alloc accounting data starts on the first page after the header
	uint32_t *memsize = (uint32_t*)( (uint8_t*)base + base->pg_size );
	// Compute the number of words used for accounting, then
	// figure out the first usable page after the accounting data.
	uint32_t pgpool = base->pg_count;
	uint32_t allocwords_page = base->pg_size / (SMALLEST_ALLOC*64);
	uint64_t *bitmap = (uint64_t*)(memsize + pgpool);
	uint32_t msp;
	int spi;

	// Compute which page we're in and get the size
	offset = (uint8_t*)addr - (uint8_t*)base;
	page = offset / base->pg_size;
	msp = memsize[page];
	size = PM_SIZE(msp);
	// Compute which sub-block we're in and compute the mb_subpool index
	block = (offset % base->pg_size) / size;
        spi = ntz(size) - 4;

	// Clear the bit in the bitmap
	if (clear_bit(bitmap + page*allocwords_page, block) < 0) {
		log_error("double free: %p\n", addr);
		abort();
	}

	// Clear the full bit (in case it was set)
	if (msp & PM_SUBPAGEFULL) {
		cmpxchg32(&memsize[page], msp, PM_GUARD_INC(msp) &~PM_SUBPAGEFULL);
	}

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
	size = PM_SIZE(memsize[page]);

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
	size = PM_SIZE(memsize[page]);
	return size;
}

void pmem_gc_mark(memblock_t *base)
{
	uint32_t *memsize = (uint32_t*)( (uint8_t*)base + base->pg_size );
	// Compute the number of words used for accounting, then
	// figure out the first usable page after the accounting data.
	uint32_t total = base->pg_size * base->pg_count;
	uint32_t pgpool = base->pg_count;
	uint32_t nbitmap = total / (SMALLEST_ALLOC * 64);
	uint32_t first = firstpage(base);
	uint64_t *bitmap = (uint64_t*)(memsize + pgpool);
	uint64_t *bitmark = bitmap + nbitmap;
	uint32_t oldval, newval;
	int i;

	// There are two kinds of marks for the garbage collector
	// 1. A mark in the memsize array for allocation of page_size or larger
	// 2. A bitmark array for suballocations less than page_size
	for(i=first; i<pgpool; i++) {
		do {
			oldval = memsize[i];
			// We only mark pg_size or greater allocations here
			if (PM_SIZE(oldval) < base->pg_size)
				break;
			newval = PM_GUARD_INC(oldval) | PM_GC_MARK;
		} while(!cmpxchg32(&memsize[i], oldval, newval));
	}
	memcpy(bitmark, bitmap, nbitmap*sizeof(uint64_t));
}

void pmem_gc_keep(memblock_t *base, void *addr)
{
	uint32_t *memsize = (uint32_t*)( (uint8_t*)base + base->pg_size );
	// Compute the number of words used for accounting, then
	// figure out the first usable page after the accounting data.
	uint32_t total = base->pg_size * base->pg_count;
	uint32_t pgpool = base->pg_count;
	uint64_t nbitmap = total / (SMALLEST_ALLOC * 64);
	uint32_t allocwords_page = base->pg_size / (SMALLEST_ALLOC*64);
	uint64_t *bitmap = (uint64_t*)(memsize + pgpool);
	uint64_t *bitmark = bitmap + nbitmap;
	uint32_t offset, page, size, block;
	uint64_t mask;
	uint32_t oldval, newval;

	if (!addr)
		return;

	// We assume exclusive access to memsize[] and bitmark[] because:
	//    If you're using the GC, you can't use free
	//    There will only be one GC running
	//    If the memsize element has a size, alloc won't interfere with it.
	// TODO: investigate making the GC and free be safe together

	offset = (uint8_t*)addr - (uint8_t*)base;
	page = offset / base->pg_size;
	size = PM_SIZE(memsize[page]);
	if (size == 1) {
		log_error("pmem_gc_keep: address %p is in a continuation\n", addr);
		abort();
	} else if (size >= base->pg_size) {
		oldval = memsize[page];
		newval = PM_GUARD_INC(oldval) & ~PM_GC_MARK;
		memsize[page] = newval;
	} else {
		// Compute which sub-block we're in
		block = (offset % base->pg_size) / size;

		// Clear the bit in the bitmap
		offset = page * allocwords_page + block/64;
		mask = 1ULL << (block % 64);

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
	uint32_t nbitmap = total / (SMALLEST_ALLOC * 64);
	uint32_t first = firstpage(base);
	uint32_t allocwords_page = base->pg_size / (SMALLEST_ALLOC*64);
	uint64_t *bitmap = (uint64_t*)(memsize + pgpool);
	uint64_t *bitmark = bitmap + nbitmap;
	uint64_t a, oldval, newval;
	int i, j, page, spi, size;
	uint32_t msval;
	void *addr;

	// First clear the PM_GC_MARK bits in the memsize array
	for(i=first; i<pgpool; i++) {
		msval = memsize[i];
		if (msval & PM_GC_MARK) {
			if (gc_free_cb) {
				addr = (uint8_t*)base + base->pg_size * i;
				gc_free_cb(user, addr);
			}

			// Don't need cmpxchg here because no one should touch the
			// continuation.
			j = i+1;
			while((memsize[j] & PM_FLAG_MASK) == PM_CONTINUATION) {
				memsize[j] = PM_GUARD_INC(memsize[j]) & ~PM_CONTINUATION;
				j++;
			}

			// Don't need cmpxchg here because no one should touch an
			// allocated block (except free and the GC)
			memsize[i] = PM_GUARD_INC(msval) & ~PM_GUARD;
			// If the lfp is -1 (no more pages), set it to
			// the index, because we're freeing it.
			if (base->mb_lfp == -1)
				base->mb_lfp = i;
		}
	}
	// Now clear all the bits in the bitmask array that have
	// their corresponding bit set in the bitmark array
	a = 0;
	for(i=first*allocwords_page; i<nbitmap; i++) {
		page = i / allocwords_page;
		msval = memsize[page];
		size = PM_SIZE(msval);
		// First check if we need to call gc_free_cb for anything
		if (gc_free_cb && bitmark[i]) {
			for(j=0; j<64; j++) {
				if (bitmark[i] & (1ULL<<j)) {
					addr = (uint8_t*)base + (base->pg_size*page +
					       size*((i%allocwords_page)*64+j));
					gc_free_cb(user, addr);
				}
			}
		}
		//if (bitmark[i]) log_bare("%d %04x %08x", i, page, bitmark[i]);

		// Clear all the bits set in the bitmark array
		do {
			oldval = bitmap[i];
			newval = oldval & ~bitmark[i];
		} while(!cmpxchg64(&bitmap[i], oldval, newval));

		// Do we have allocations in this page?
		a |= newval;

		// If we had frees in this suballoction, clear the
		// subpagefull flag in the memsize array
		if (bitmark[i]) {
			// this cmpxchg probably can't fail because PM_SUBPAGEFULL will
			// keep the allocator away from this page
			if (msval & PM_SUBPAGEFULL)
				cmpxchg32(&memsize[page], msval, msval & ~PM_SUBPAGEFULL);

			// If the mb_subpool is -1 set it to this page, since
			// there are free chunks
			spi = ntz(size) - 4;
			if (base->mb_subpool[spi] == -1)
				base->mb_subpool[spi] = page;
		}
		if ((i+1)%allocwords_page==0) {
			// If we've scanned a whole page's worth of bitmap
			// bits and none were set, we might be able to
			// free the page back to the page allocator.
			if (a == 0) {
				if (size >= SMALLEST_ALLOC && size < base->pg_size) {
					spi = ntz(size) - 4;
					if (base->mb_subpool[spi] == -1)
						base->mb_subpool[spi] = 0;
					if (base->mb_lfp == -1)
						base->mb_lfp = i;
					// ok for this cmpxchg to fail.  If suballoc is
					// doing an allocation, failing this cmpxchg will prevent
					// us from freeing a page we don't want to free.
					cmpxchg32(&memsize[page], msval, PM_GUARD_INC(msval)&PM_GUARD);
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
	uint32_t allocwords_page = base->pg_size / (SMALLEST_ALLOC*64);
	uint64_t *bitmap = (uint64_t*)(memsize + pgpool);
	
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
				if (bitmap[i*allocwords_page + j/64] & (1ULL << (j%64))) {
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
	uint32_t allocwords_page = base->pg_size / (SMALLEST_ALLOC*64);
	uint64_t *bitmap = (uint64_t*)(memsize + pgpool);
	uint32_t sz, flags;
	int i, j;
	int a, bits;
	uint32_t fpg = 0;
	uint32_t total = 0;

	log_bare("Info for memblock %p: offset=%016llx size=%08llx", base, base->mb_offset, base->mb_size);
	log_bare("Pages 0 to 0x%04x contain memory allocation structs", first);
	for(i=first; i<pgpool; i++) {
		sz = memsize[i];
		flags = sz & PM_FLAG_MASK;
		sz = PM_SIZE(sz);
		if (flags & PM_CONTINUATION) continue;
		if (sz==0) {
			fpg++;
			continue;
		}
		if (sz<4096) {
			bits = base->pg_size / sz;
			a = 0;
			for(j=0; j<bits; j++) {
				if (bitmap[i*allocwords_page + j/64] & (1ULL << (j%64)))
					a++;
			}
			total += a*sz;
			log_bare("page 0x%04x: size=%04x flags=%x (%d/%d allocated)", i, sz, flags, a, bits);
		} else {
			total += sz;
			log_bare("page 0x%04x: size=%04x flags=%x", i, sz, flags);
		}
	}
	log_bare("Bytes allocated: %d / %d", total, base->mb_size);
	log_bare("Free pages: %d", fpg);
	log_bare("mb_lfp=%04x mb_subpool=%04x %04x %04x %04x %04x %04x %04x %04x",
			base->mb_lfp,
			base->mb_subpool[0], base->mb_subpool[1],
			base->mb_subpool[2], base->mb_subpool[3],
			base->mb_subpool[4], base->mb_subpool[5],
			base->mb_subpool[6], base->mb_subpool[7]);
	log_bare("");
}
