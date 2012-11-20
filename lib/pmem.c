#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pongo/stdtypes.h>
#include <pongo/pmem.h>
#include <pongo/atomic.h>
#include <pongo/mmfile.h>
#include <pongo/misc.h>
#include <pongo/log.h>

/*
** Lock-free memory allocator for mmfiles.
**
** Based loosely on Scalable Lock-Free Dynamic Memory Allocation
** by Maged M. Michael
**
*/

#define SMALLEST_POOL_ALLOC 1024
#define NR_CHUNKS(sz) ((65536-80)/(8+sz))

void *(*pmem_more_memory)(mmfile_t *mm, uint32_t *size);
uint8_t free_pattern;
uint64_t procheap_timeout = 60000000;

static unsigned clssize[NR_SZCLS] = {
    16, 24, 32, 40, 48, 64, 80, 96,
    128, 160, 192, 256, 384, 512, 768
};

mempool_t *pmem_pool_init(void *addr, uint32_t size)
{
    mempool_t *pool = (mempool_t*)addr;

    // CJF -- shouldn't need to do this
    //memset(pool, 0, size);
    pool->signature = SIG_MEMPOOL;
    pool->next = 0;
    pool->size = size;
    pool->desc[0].s_ofs = offsetof(mempool_t, desc[1]);
    pool->desc[0].e_ofs = size;
    return pool;
}

void *pmem_pool_alloc(mempool_t *pool, uint32_t size)
{
    unsigned p, i, n, sz, psz;
    pdescr_t desc, newdesc;
    uint8_t *ret;
    poolblock_t *pb;

    // Add 2 64-bit words to hold the alloation size and a linked
    // list pointer.
    size += sizeof(*pb);

    do {
        ret = NULL;
        psz = -1;

        // Scan the pool descriptors for the smallest block that will
        // satisfy the request
        n = (pool->desc[0].s_ofs - offsetof(mempool_t, desc)) / sizeof(pdescr_t);
        for(i=0; i<n; i++) {
            desc = pool->desc[i];
            sz = desc.e_ofs - desc.s_ofs;
            if (sz >= size && sz < psz) {
                psz = sz;
                p = i;
            }
        }
        if (psz == -1)
            break;

        newdesc = desc;
        newdesc.e_ofs -= size;
        ret = ((uint8_t*)pool + newdesc.e_ofs);
        if (newdesc.e_ofs - newdesc.s_ofs < SMALLEST_POOL_ALLOC) {
            // if we would cut the block into an unusable chunk,
            // then just claim the whole thing.
            size += newdesc.e_ofs - newdesc.s_ofs;
            // Can't mark the 0-th descriptor as free
            if (p) newdesc.all = 0;
        }
    } while (!cmpxchg64(&pool->desc[p], desc.all, newdesc.all));

    if (ret) {
        assert((newdesc.e_ofs & 7) == 0);
        //CJF -- should need this
        memset(ret, 0, size);
        pb = (poolblock_t*)ret;
        // The first word is the linked list pointer, the second is
        // the allocation size
        pb->next = 0;
        pb->size = size;
        pb->type = 0;
        pb->alloc = 1;
        pb->gc = 0;
        pb->pool = newdesc.e_ofs >> 3;
        ret = (void*)(pb+1);
    }
    return ret;
}

int pmem_pool_free(void *addr)
{
    mempool_t *pool;
    poolblock_t *pb;
    unsigned i, n, z, p, offset;
    int full;
    pdescr_t desc, olddesc, newdesc;

    if (!addr) return 0;

    // Back up to actual start of block, compute location of pool
    pb = (poolblock_t*)addr - 1;
    offset = pb->pool << 3;
    pool = (mempool_t*)((uint8_t*)pb - offset);
    do {
        // Read the first descriptor and compute the number of
        // descriptors available and whether or not the pool is full
        z = p = -1;
        olddesc = pool->desc[0];
        n = (olddesc.s_ofs - offsetof(mempool_t, desc)) / sizeof(pdescr_t);
        full = (olddesc.s_ofs == olddesc.e_ofs);

        // Scan for a free descriptor or a descriptor that describes
        // a region adjacent to the one being freed.
        for(i=0; i<n; i++) {
            desc = pool->desc[i];
            if (desc.all == 0)
                z = p = i;
            if (desc.e_ofs == offset) {
                p = i; break;
            }
        }

        // If no free descriptors were found, either abort wth an error
        // (pool full) or allocate a new descriptor.
        if (p == -1) {
            if (full)
                return -1;
            newdesc = olddesc;
            newdesc.s_ofs += sizeof(uint64_t);

            if (!cmpxchg64(&pool->desc[0], olddesc.all, newdesc.all))
                continue;
            // Treat it like it reads zero.  It *should* be zero since
            // its unallocated memory
            z = p = n;
        }

        if (z == p) {
            // If the desciptor was free (contained zero), then
            // desc=0 and newdesc=memregion
            desc.all = 0;
            newdesc.s_ofs = offset;
            newdesc.e_ofs = offset + pb->size;
        } else {
            // If the descriptor describes a memory block adjacent to
            // the one being freed, adjust to describe new block
            newdesc = desc;
            newdesc.e_ofs = offset + pb->size;
        }
    } while(!cmpxchg64(&pool->desc[p], desc.all, newdesc.all));
    return 0;
}



superblock_t *pmem_sb_init(mempool_t *pool, uint32_t blksz, uint32_t count)
{
    superblock_t *sb;
    memblock_t *mb;
    uint8_t *p;
    unsigned i;

    // Allocate a big block out of the mempool
    sb = pmem_pool_alloc(pool, sizeof(*sb) + count*(blksz+sizeof(*mb)));
    if (!sb)
        return NULL;

    sb->signature = SIG_SUPERBLK;
    sb->next = 0;
    p = (uint8_t*)(sb+1);
    // Chain all of the smaller blocks together
    for(i=0; i<count; i++, p+=blksz+sizeof(*mb)) {
        mb = (memblock_t*)p;
        if (i)
            mb->next = i-1;
        mb->sbofs = (uintptr_t)mb - (uintptr_t)sb;
        mb->type = 1;
        mb->alloc = 0;
        mb->suggest = 0;
        mb->gc = 0;
        mb->_resv = 0;
    }
    sb->desc.free = count-1;
    sb->desc.count = count;
    sb->desc.size = blksz;
    sb->desc.tag = 0;
    sb->size = blksz;
    sb->total = count;
    return sb;
}

void *pmem_sb_alloc(superblock_t *sb)
{
    bdescr_t oldval, newval;
    uint8_t *p = (uint8_t*)(sb+1);
    memblock_t *mb;
    void *ret;

    // pop a block off of the superblock's free list
    do {
        oldval = sb->desc;
        if (!oldval.count)
            return NULL;
        newval = oldval;

        mb = (memblock_t*)(p + oldval.free * (sizeof(*mb) + oldval.size));
        newval.free = mb->next;
        newval.count--;
        newval.tag++;
    } while(!cmpxchg64(&sb->desc, oldval.all, newval.all));

    assert(mb->alloc == 0);
    mb->alloc = 1;
    ret = (void*)(mb+1);
    // CJF -- shouldn't need this
    memset(ret, 0, sb->size);
    return ret;
}

void pmem_sb_free(superblock_t *sb, void *addr)
{
    bdescr_t oldval, newval;
    memblock_t *mb;
    unsigned blk;

    if (!addr)
        return;

    // push a block back onto the superblock's free list
    mb = (memblock_t*)addr - 1;
    if (!sb) {
        sb = (superblock_t*)((uint8_t*)mb - mb->sbofs);
    }
    assert(mb->alloc);
    // CJF debug
    memset(addr, 0, sb->size);
    mb->alloc = 0;
    mb->gc = 0;
    mb->suggest = 0;
    blk = ((uintptr_t)mb - (uintptr_t)(sb+1)) / (sb->size + sizeof(*mb));
    do {
        newval = oldval = sb->desc;
        newval.count++;
        newval.free = blk;
        newval.tag++;
        mb->next = oldval.free;
    } while(!cmpxchg64(&sb->desc, oldval.all, newval.all));
}

void *pmem_more(mmfile_t *mm, volatile mlist_t *pool)
{
    void *more = NULL;
    uint32_t newsize;
    mempool_t *mp;

    // Try to get more memory
    if (pmem_more_memory) 
        more = pmem_more_memory(mm, &newsize);
    if (more) {
        mp = pmem_pool_init(more, newsize);
        do {
            mp->next = pool->freelist;
        } while(!cmpxchg64(&pool->freelist, mp->next, __offset(mm, mp)));
    }
    return more;
}

void *pmem_pool_helper(mmfile_t *mm, memheap_t *heap, uint32_t size)
{
    volatile mlist_t *pool = &heap->pool;
    uint64_t freelist;
    mempool_t *mp;
    void *ret = NULL;

    for(;;) {
        // Get the mempool pointer from the freelist and try an
        // allocation
        freelist = pool->freelist;
        mp = __ptr(mm, freelist);
        if (mp && (ret=pmem_pool_alloc(mp, size)) != NULL)
            break;

        // if the allocation failed
        if (mp) {
            // Push the pool onto the fulllist
            if (cmpxchg64(&pool->freelist, freelist, mp->next)) {
                // freelist is now pointing at the "full" mp
                do {
                    mp->next = pool->fulllist;
                } while(!cmpxchg64(&pool->fulllist, mp->next, freelist));
            }
        } else {
            // Try to get more memory
#if 0
            if (!pmem_more(mm, pool))
                break;
#endif
            pmem_more(mm, &heap->pool);
        }
    }
    return ret;
}

void *pmem_sb_helper(mmfile_t *mm, memheap_t *heap, volatile mlist_t *memory, int cls)
{
    uint32_t sz;
    uint64_t freelist;
    superblock_t *sb;
    void *ret = NULL;

    for(;;) {
        // Get the superblock pointer from the freelist and try to allocate
        // memory
        freelist = memory->freelist;
        sb = __ptr(mm, freelist);
        if (sb && (ret=pmem_sb_alloc(sb)) != NULL)
            break;

        // If allocation fails
        if (sb) {
            // Get the next superblock from the freelist and put the
            // current superblock onto the full list
            if (cmpxchg64(&memory->freelist, freelist, sb->next)) {
                // local copy of freelist currently points to full superblock
                do {
                    sb->next = memory->fulllist;
                } while(!cmpxchg64(&memory->fulllist, sb->next, freelist));
            }
        } else {
            // First try getting a block from procheap zero.
            do {
                freelist = heap->procheap[0].szcls[cls].freelist;
                sb = __ptr(mm, freelist);
                if (!sb) break;
            } while(!cmpxchg64(&heap->procheap[0].szcls[cls].freelist, freelist, sb->next));

            // If there was no block available, allocate one from the pool
            if (!sb) {
                sz = clssize[cls];
                sb = pmem_sb_init(__ptr(mm, heap->pool.freelist), sz, NR_CHUNKS(sz));
            }

            if (sb) {
                // Got a block, put it onto the freelist
                do {
                    sb->next = memory->freelist;
                } while(!cmpxchg64(&memory->freelist, sb->next, __offset(mm, sb)));
            } else {
                // Try to get more memory
#if 0
                if (!pmem_more(mm, &heap->pool))
                    break;
#endif
                pmem_more(mm, &heap->pool);
            }
        }
    }
    return ret;
}

static inline int pmem_heap(memheap_t *heap)
{
    static int ph;
    int pid = getpid();
    if (!ph) {
        ph = 1 + pid % (heap->nr_procheap-1);
        log_bare("pid %d using heap %d", pid, ph);
    }
    return ph;
}

void *pmem_alloc(mmfile_t *mm, memheap_t *heap, uint32_t sz)
{
    int ph, cls;
    procheap_t *procheap;
    volatile mlist_t *memory;
    poolblock_t *pb;
    void *ret = NULL;

    for(cls=0; cls<NR_SZCLS; cls++) {
        if (sz <= clssize[cls])
            break;
    }

    if (cls < NR_SZCLS) {
        ph = pmem_heap(heap);
        procheap = &heap->procheap[ph];
        procheap->last_used = utime_now();
        memory = &procheap->szcls[cls];
        ret = pmem_sb_helper(mm, heap, memory, cls);
    } else {
        // Round to nearest kilobyte
        sz = (sz + 0x3FF) & ~0x3FF; 
        ret = pmem_pool_helper(mm, heap, sz);

        // Put the block on the allocated list
        pb = (poolblock_t*)ret-1;
        do {
            pb->next = heap->pool_alloc;
        } while(!cmpxchg64(&heap->pool_alloc, pb->next, __offset(mm, pb)));
    }
    return ret;
}

void pmem_retire(mmfile_t *mm, memheap_t *heap, int ph)
{
    int i;
    volatile mlist_t *memory, *retire;
    uint64_t oldval, newval;
    superblock_t *sb;

    if (ph == 0)
        ph = pmem_heap(heap);

    log_debug("Retiring heap %d", ph);
    for(i=0; i<NR_SZCLS; i++) {
        retire = &heap->procheap[0].szcls[i];
        memory = &heap->procheap[ph].szcls[i];
        // Grab the freelist off of this pid's procheap
        // and push each item onto procheap zero.
        do {
            oldval = memory->freelist;
        } while(!cmpxchg64(&memory->freelist, oldval, 0));

        while(oldval) {
            newval = oldval;
            sb = __ptr(mm, newval);
            oldval = sb->next;
            do {
                sb->next = retire->freelist;
            } while(!cmpxchg64(&retire->freelist, sb->next, newval));
        }

        // Grab the fulllist off of this pid's procheap
        // and push each item onto procheap zero.
        do {
            oldval = memory->fulllist;
        } while(!cmpxchg64(&memory->fulllist, oldval, 0));

        while(oldval) {
            newval = oldval;
            sb = __ptr(mm, newval);
            oldval = sb->next;
            do {
                sb->next = retire->fulllist;
            } while(!cmpxchg64(&retire->fulllist, sb->next, newval));
        }
    }
}

void pmem_gc_mark_sb(mmfile_t *mm, superblock_t *sb)
{
    memblock_t *mb;
    uint8_t *p;
    unsigned i;
    
    while(sb) {
        p = (uint8_t*)(sb+1);
        for(i=0; i<sb->total; i++, p+=sb->size+sizeof(*mb)) {
            mb = (memblock_t*)p;
            if (mb->alloc) {
                mb->gc = 1;
            }
        }
        sb->gc = 1;
        sb->suggest = 0;
        sb = __ptr(mm, sb->next);
    }
}

void pmem_gc_mark_suggest(mmfile_t *mm, superblock_t *sb)
{
    memblock_t *mb;
    uint8_t *p;
    unsigned i;
    
    while(sb) {
        if (sb->suggest) {
            p = (uint8_t*)(sb+1);
            for(i=0; i<sb->total; i++, p+=sb->size+sizeof(*mb)) {
                mb = (memblock_t*)p;
                if (mb->suggest) {
                    mb->gc = 1;
                }
            }
            sb->gc = 1;
            sb->suggest = 0;
        }
        sb = __ptr(mm, sb->next);
    }
}

void pmem_gc_mark(mmfile_t *mm, memheap_t *heap, int suggest)
{
    poolblock_t *pb;
    unsigned i, j;

    for(i=0; i<heap->nr_procheap; i++) {
        for(j=0; j<NR_SZCLS; j++) {
            if (suggest) {
                pmem_gc_mark_suggest(mm,
                        __ptr(mm, heap->procheap[i].szcls[j].freelist));
                pmem_gc_mark_suggest(mm,
                        __ptr(mm, heap->procheap[i].szcls[j].fulllist));
            } else {
                pmem_gc_mark_sb(mm,
                        __ptr(mm, heap->procheap[i].szcls[j].freelist));
                pmem_gc_mark_sb(mm,
                        __ptr(mm, heap->procheap[i].szcls[j].fulllist));
            }
        }
    }
    
    pb = __ptr(mm, heap->pool_alloc);
    while(pb) {
        pb->gc = 1;
        pb = __ptr(mm, pb->next);
    }
}

int pmem_gc_free_sb(superblock_t *sb, gcfreecb_t callback, void *user)
{
    memblock_t *mb;
    uint8_t *p;
    unsigned i, n;
    

    sb->gc = 0;
    p = (uint8_t*)(sb+1);
    for(n=i=0; i<sb->total; i++, p+=sb->size+sizeof(*mb)) {
        mb = (memblock_t*)p;
        if (mb->gc) {
            if (callback) callback(user, mb+1);
            pmem_sb_free(sb, mb+1);
            n++;
        }
    }
    return n;
}

void pmem_gc_free_sblist(mmfile_t *mm, volatile mlist_t *memory, gcfreecb_t callback, void *user)
{
    uint64_t oldval, newval;
    superblock_t *sb;
    int n;

#if 1
    // First get the freelist to ourselves to no one can mess with it
    do {
        oldval = memory->freelist;
    } while(!cmpxchg64(&memory->freelist, oldval, 0));
#else
    oldval = memory->freelist;
#endif

    // Now, walk the free list, collect garbage and put blocks
    // back onto the freelist
    newval = oldval;
    sb = __ptr(mm, newval);
    while(sb) {
        pmem_gc_free_sb(sb, callback, user);
        oldval = sb->next;
#if 1
        do {
            sb->next = memory->freelist;
        } while(!cmpxchg64(&memory->freelist, sb->next, newval));
#endif
        newval = oldval;
        sb = __ptr(mm, newval);
    }

    // First get the fulllist to ourselves to no one can mess with it
    do {
        oldval = memory->fulllist;
    } while(!cmpxchg64(&memory->fulllist, oldval, 0));

    // Now, walk the full list, collect garbage and put blocks back onto
    // the freelist or fulllist depending on whether anything was freed.
    newval = oldval;
    sb = __ptr(mm, newval);
    while(sb) {
        n = pmem_gc_free_sb(sb, callback, user);
        oldval = sb->next;
        if (n) {
            do {
                sb->next = memory->freelist;
            } while(!cmpxchg64(&memory->freelist, sb->next, newval));
        } else {
            do {
                sb->next = memory->fulllist;
            } while(!cmpxchg64(&memory->fulllist, sb->next, newval));
        }
        newval = oldval;
        sb = __ptr(mm, newval);
    }
}

void pmem_gc_free(mmfile_t *mm, memheap_t *heap, int fast, gcfreecb_t cb, void *user)
{
    poolblock_t *pb;
    unsigned i, j;
    uint64_t oldval, newval;
    uint64_t now;

    free_pattern = fast ? 0xFE : 0xFA;
    for(i=0; i<heap->nr_procheap; i++) {
        for(j=0; j<NR_SZCLS; j++) {
            pmem_gc_free_sblist(mm, &heap->procheap[i].szcls[j], cb, user);
        }
    }

    if (fast)
        return;

    now = utime_now();
    for(i=1; i<heap->nr_procheap; i++) {
        if (heap->procheap[i].last_used - now >= procheap_timeout) {
            pmem_retire(mm, heap, i);
        }
    }
    
    // First get the pool_alloc list to ourselves to no one can mess with it
    do {
        oldval = heap->pool_alloc;
    } while(!cmpxchg64(&heap->pool_alloc, oldval, 0));

    newval = oldval;
    pb = __ptr(mm, newval);
    while(pb) {
        oldval = pb->next;
        if (pb->gc) {
            if (cb) cb(user, pb+1);
            pmem_pool_free(pb+1);
            // FIXME: a mempool with free space should be moved to the
            // front of the mempool freelist
        } else {
            do {
                pb->next = heap->pool_alloc;
            } while(!cmpxchg64(&heap->pool_alloc, pb->next, newval));
        }
        newval = oldval;
        pb = __ptr(mm, newval);
    }
}

static void print_mempool(mmfile_t *mm, mempool_t *pool)
{
    unsigned i, n, s, e, total;
    uint64_t start = __offset(mm, pool);
    log_bare("mempool addr=%08"PRIx64", size=0x%x next=0x%" PRIx64, start,
            pool->size, pool->next);
    n = (pool->desc[0].s_ofs - offsetof(mempool_t, desc)) / sizeof(pdescr_t);
    log_bare("Free regions: %d", n);
    for(total=i=0; i<n; i++) {
        s = pool->desc[i].s_ofs;
        e = pool->desc[i].e_ofs;
        total += (e-s);
        log_bare("    start=%08"PRIx64" end=%08"PRIx64" size=0x%08x", start+s, start+e, e-s);
    }
    log_bare("    Total free: 0x%08x", total);
}


void pmem_print_mem(mmfile_t *mm, memheap_t *heap)
{
    mempool_t *pool;
    poolblock_t *pb;
    superblock_t *sb;
    volatile mlist_t *memory;
    uint64_t p;
    uint32_t total, free;
    int i, j;

    for(i=0; i<heap->nr_procheap; i++) {
        for(j=0; j<NR_SZCLS; j++) {
            memory = &heap->procheap[i].szcls[j];

            log_bare("=== Freelist for heap %d szcls %d ===", i, j);
            total = free = 0;
            p = memory->freelist;
            while(p) {
                sb = __ptr(mm, p);
                log_bare("superblock at %08" PRIx64 ": size=0x%x, %d/%d free", __offset(mm, sb), sb->size, sb->desc.count, sb->total);
                total += sb->total;
                free += sb->desc.count;
                p = sb->next;
            }
            log_bare("Total: %d/%d blocks free (%d/%d bytes)", free, total, free*sb->size, total*sb->size);

            log_bare("=== Fulllist for heap %d szcls %d ===", i, j);
            total = free = 0;
            p = memory->fulllist;
            while(p) {
                sb = __ptr(mm, p);
                log_bare("superblock at %08" PRIx64 ": size=0x%x, %d/%d free", __offset(mm, sb), sb->size, sb->desc.count, sb->total);
                total += sb->total;
                free += sb->desc.count;
                p = sb->next;
            }
            log_bare("Total: %d/%d blocks free (%d/%d bytes)", free, total, free*sb->size, total*sb->size);
        }
    }

    log_bare("=== Pool Alloc List ===");
    p = heap->pool_alloc;
    while(p) {
        pb = __ptr(mm, p);
        printf("poolblock at %08" PRIx64 " is 0x%x bytes\n", __offset(mm, pb), pb->size);
        p = pb->next;
    }

    log_bare("=== Pool Free List ===");
    p = heap->pool.freelist;
    while(p) {
        pool = __ptr(mm, p);
        print_mempool(mm, pool);
        p = pool->next;
    }

    log_bare("=== Pool Full List ===");
    p = heap->pool.fulllist;
    while(p) {
        pool = __ptr(mm, p);
        print_mempool(mm, pool);
        p = pool->next;
    }
}

// vim: ts=4 sts=4 sw=4 expandtab:
