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

mempool_t *pmem_pool_init(void *addr, uint32_t size)
{
    mempool_t *pool = (mempool_t*)addr;

    memset(pool, 0, size);
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
    uint64_t *ret;
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
        ret = (uint64_t*)((uint8_t*)pool + newdesc.e_ofs);
        if (newdesc.e_ofs - newdesc.s_ofs < SMALLEST_POOL_ALLOC) {
            // if we would cut the block into an unusable chunk,
            // then just claim the whole thing.
            size += newdesc.e_ofs - newdesc.s_ofs;
            // Can't mark the 0-th descriptor as free
            if (p) newdesc.all = 0;
        }
    } while (!cmpxchg64(&pool->desc[p], desc.all, newdesc.all));

    assert((newdesc.e_ofs & 7) == 0);
    pb = (poolblock_t*)ret;
    // The first word is the linked list pointer, the second is
    // the allocation size
    pb->next = 0;
    pb->size = size;
    pb->type = 0;
    pb->alloc = 1;
    pb->gc = 0;
    pb->pool = newdesc.e_ofs >> 3;
    return (void*)(pb+1);
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

void pmem_pool_print(mempool_t *pool)
{
    unsigned i, n, s, e, total;
    log_bare("mempool addr=%p, size=0x%x next=0x%" PRIx64, pool, pool->size, pool->next);
    n = (pool->desc[0].s_ofs - offsetof(mempool_t, desc)) / sizeof(pdescr_t);
    log_bare("Free regions: %d", n);
    for(total=i=0; i<n; i++) {
        s = pool->desc[i].s_ofs;
        e = pool->desc[i].e_ofs;
        total += (e-s);
        log_bare("    start=0x%08x end=0x%08x size=0x%08x", s, e, e-s);
    }
    log_bare("Total free: 0x%08x", total);
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
        mb->gc = 0;
        mb->_resv = 0;
    }
    sb->desc.free = count-1;
    sb->desc.count = count;
    sb->desc.size = blksz;
    sb->desc.tag = 0;
    return sb;
}

void *pmem_sb_alloc(superblock_t *sb)
{
    bdescr_t oldval, newval;
    uint8_t *p = (uint8_t*)(sb+1);
    memblock_t *mb;

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

    mb->alloc = 1;
    return (void*)(mb+1);
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
    mb->alloc = 0;
    mb->gc = 0;
    blk = mb - (memblock_t*)(sb+1);
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
            if (!pmem_more(mm, pool))
                break;
        }
    }
    return ret;
}

void *pmem_sb_helper(mmfile_t *mm, memheap_t *heap, volatile mlist_t *memory, uint32_t sz)
{
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
            // Allocate a new superblock and push it onto the freelist
            sb = pmem_sb_init(__ptr(mm, heap->pool.freelist), sz, NR_CHUNKS(sz));
            if (sb) {
                do {
                    sb->next = memory->freelist;
                } while(!cmpxchg64(&memory->freelist, sb->next, __offset(mm, sb)));
            } else {
                // Try to get more memory
                if (!pmem_more(mm, &heap->pool))
                    break;
            }
        }
    }
    return ret;
}

void *pmem_alloc(mmfile_t *mm, memheap_t *heap, uint32_t sz)
{
    int ph;
    volatile mlist_t *memory;
    poolblock_t *pb;
    void *ret = NULL;

    ph = gettid() % heap->nr_procheap;
    if (sz <= 16) {
        sz = 16;
        memory = &heap->procheap[ph].szcls[0];
    } else if (sz <= 32) {
        sz = 32;
        memory = &heap->procheap[ph].szcls[1];
    } else if (sz <= 64) {
        sz = 64;
        memory = &heap->procheap[ph].szcls[2];
    } else if (sz <= 128) {
        sz = 128;
        memory = &heap->procheap[ph].szcls[3];
    } else if (sz <= 256) {
        sz = 256;
        memory = &heap->procheap[ph].szcls[4];
    } else if (sz <= 512) {
        sz = 512;
        memory = &heap->procheap[ph].szcls[5];
    } else {
        // Round to nearest kilobyte
        sz = (sz + 0x3FF) & ~0x3FF; 
    }

    if (sz <= 512) {
        ret = pmem_sb_helper(mm, heap, memory, sz);
    } else {
        ret = pmem_pool_helper(mm, heap, sz);

        // Put the block on the allocated list
        pb = (poolblock_t*)ret;
        do {
            pb->next = heap->pool_alloc;
        } while(!cmpxchg64(&heap->pool_alloc, pb->next, __offset(mm, ret)));
    }
    return ret;
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
        sb = __ptr(mm, sb->next);
    }
}

void pmem_gc_mark(mmfile_t *mm, memheap_t *heap)
{
    poolblock_t *pb;
    unsigned i, j;

    for(i=0; i<heap->nr_procheap; i++) {
        for(j=0; j<NR_SZCLS; j++) {
            pmem_gc_mark_sb(mm, __ptr(mm, heap->procheap[i].szcls[j].freelist));
            pmem_gc_mark_sb(mm, __ptr(mm, heap->procheap[i].szcls[j].fulllist));
        }
    }
    
    pb = __ptr(mm, heap->pool_alloc);
    while(pb) {
        pb->gc = 1;
        pb = __ptr(mm, pb->next);
    }
}

void pmem_gc_suggest(void *addr)
{
    memblock_t *mb;
    superblock_t *sb;

    if (!addr) return;

    mb = (memblock_t*)addr - 1;
    if (mb->type == 1) {
        mb->gc = 1;
        sb = (superblock_t*)((uint8_t*)mb - mb->sbofs);
        sb->gc = 1;
    }
}

int pmem_gc_free_sb(superblock_t *sb)
{
    memblock_t *mb;
    uint8_t *p;
    unsigned i, n;
    

    sb->gc = 0;
    p = (uint8_t*)(sb+1);
    for(n=i=0; i<sb->total; i++, p+=sb->size+sizeof(*mb)) {
        mb = (memblock_t*)p;
        if (mb->gc) {
            pmem_sb_free(sb, mb);
            n++;
        }
    }
    return n;
}

void pmem_gc_free_sblist(mmfile_t *mm, volatile mlist_t *memory)
{
    uint64_t oldval, newval;
    superblock_t *sb;
    int n;

    // First get the freelist to ourselves to no one can mess with it
    do {
        oldval = memory->freelist;
    } while(!cmpxchg64(&memory->freelist, oldval, 0));

    // Now, walk the free list, collect garbage and put blocks
    // back onto the freelist
    newval = oldval;
    sb = __ptr(mm, oldval);
    while(sb) {
        pmem_gc_free_sb(sb);
        oldval = sb->next;
        do {
            sb->next = memory->freelist;
        } while(!cmpxchg64(&memory->freelist, sb->next, newval));
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
        n = pmem_gc_free_sb(sb);
        oldval = sb->next;
        if (n) {
            do {
                oldval = memory->freelist;
            } while(!cmpxchg64(&memory->freelist, sb->next, newval));
        } else {
            do {
                oldval = memory->fulllist;
            } while(!cmpxchg64(&memory->fulllist, sb->next, newval));
        }
        newval = oldval;
        sb = __ptr(mm, newval);
    }
}

void pmem_gc_free(mmfile_t *mm, memheap_t *heap, int fast)
{
    poolblock_t *pb;
    unsigned i, j;
    uint64_t oldval, newval;

    for(i=0; i<heap->nr_procheap; i++) {
        for(j=0; j<NR_SZCLS; j++) {
            pmem_gc_free_sblist(mm, &heap->procheap[i].szcls[j]);
        }
    }

    if (fast)
        return;
    
    // First get the pool_alloc list to ourselves to no one can mess with it
    do {
        oldval = heap->pool_alloc;
    } while(!cmpxchg64(&heap->pool_alloc, oldval, 0));

    newval = oldval;
    pb = __ptr(mm, newval);
    while(pb) {
        oldval = pb->next;
        if (pb->gc) {
            pmem_pool_free(pb);
            // FIXME: a mempool with free space should be moved to the
            // front of the mempool list
        } else {
            do {
                pb->next = heap->pool_alloc;
            } while(!cmpxchg64(&heap->pool_alloc, pb->next, newval));
        }
        newval = oldval;
        pb = __ptr(mm, newval);
    }
}

// vim: ts=4 sts=4 sw=4 expandtab:
