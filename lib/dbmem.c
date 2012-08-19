#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#ifndef WIN32
#include <unistd.h>
#endif

#include <pongo/dbmem.h>
#include <pongo/dbtypes.h>
#include <pongo/pidcache.h>
#include <pongo/pmem.h>
#include <pongo/misc.h>
#include <pongo/log.h>

#undef SCANNING_GC
#define MARKING_GC 1

pgctx_t *dbctx[NR_DB_CONTEXT];

#define _dblockop(ctx, op, lock) \
	mm_lock(&(ctx)->mm, (op), \
		_offset(ctx, &(lock)), sizeof(lock));

pgctx_t *dbfindctx(const char *filename)
{
	int i;
	for(i=0; i<NR_DB_CONTEXT; i++) {
		if (!strcmp(filename, dbctx[i]->mm.filename))
			return dbctx[i];
	}
	return NULL;
}

/*
 * The memblock_t is the unit of memory upon which the allocator and
 * operates.  A file will have several memblocks as it grows.
 *
 * See pmem.[ch] for details on how memblocks work.
 *
 * You must _addmemblock for each new memblock before _ptr and _offset
 * will work.  They'll crash if you don't.
 */
static void _addmemblock(pgctx_t *ctx, memblock_t *mb)
{
	int i;
	if (ctx->nr_mb % 16 == 0) {
		ctx->mb = realloc(ctx->mb, sizeof(memblock_t*)*(ctx->nr_mb+16));
		ctx->mb_offset = realloc(ctx->mb_offset, sizeof(memblock_t*)*(ctx->nr_mb+16));
	}
	assert(ctx->mb != NULL && ctx->mb_offset != NULL);

	// The "mb" list is sorted by pointer address
	for(i=0; i<ctx->nr_mb; i++) {
		if (mb < ctx->mb[i]) {
			memmove(&ctx->mb[i+1], &ctx->mb[i], (ctx->nr_mb-i)*sizeof(mb));
			ctx->mb[i] = mb;
			break;
		}
	}
	if (i==ctx->nr_mb)
		ctx->mb[ctx->nr_mb] = mb;

	// "mb_offset" is sorted by offset.
	// We assume that _addmemblock is always adding memblocks at greater
	// file offsets than previous memblocks.
	ctx->mb_offset[ctx->nr_mb] = mb;
	ctx->nr_mb++;
}

void dbmem_info(pgctx_t *ctx)
{
	int i;
	for(i=0; i<ctx->nr_mb; i++)
		pmem_info(ctx->mb[i]);
}

#ifdef WANT_UUID_TYPE
dbtype_t *_newkey(pgctx_t *ctx, dbtype_t *value)
{
	return dbuuid_new(ctx, NULL);
}
#endif

pgctx_t *dbfile_open(const char *filename, uint32_t initsize)
{
	int ret, i;
	dbroot_t *r;
	pgctx_t *ctx;
	dbtype_t *c;
	memblock_t *mb;
	uint64_t offset;
	if (initsize == 0) initsize = 16;

	initsize *= 1024*1024;
	// Create a new context
	ctx = malloc(sizeof(*ctx));
	memset(ctx, 0, sizeof(*ctx));
	for(i=0; i<NR_DB_CONTEXT; i++) {
		if (dbctx[i] == NULL) {
			dbctx[i] = ctx;
			break;
		}
	}
	ret = mm_open(&ctx->mm, filename, initsize);
	if (ret < 0)
		return NULL;
	ctx->root = ctx->mm.map[0].ptr;
#ifdef WANT_UUID_TYPE
	ctx->newkey = _newkey;
#endif

	if (strcmp((char*)ctx->root->signature, DBROOT_SIG)) {
		r = ctx->root;
		_addmemblock(ctx, (memblock_t*)r);
		r->mb_offset = 0;
		r->mb_size = initsize;
		r->pg_size = 4096;
		r->pg_count = initsize / 4096;
		strcpy((char*)r->signature, DBROOT_SIG);

		// Create the root data dictionary
		ctx->data = dbcollection_new(ctx);
		r->data = _offset(ctx, ctx->data);

		// Not sure about the atom cache...
		ctx->cache = NULL;
		ctx->cache = dbcache_new(ctx, 0, 0);
		r->cache = _offset(ctx, ctx->cache);

		ctx->pidcache = NULL;
		r->pidcache = _offset(ctx, dbcollection_new(ctx));
		
		// Create const true/false objects
		c = dballoc(ctx, 16);
		c->type = Boolean; c->bval = 0;
		r->booleans[0] = _offset(ctx, c);

		c = dballoc(ctx, 16);
		c->type = Boolean; c->bval = 1;
		r->booleans[1] = _offset(ctx, c);

		r->meta.chunksize = initsize;
		r->meta.id = _offset(ctx, dbstring_new(ctx, "_id", 3));
	} else {

		for(offset=0; offset<ctx->mm.size; offset+=mb->mb_size) {
			mb = (memblock_t*)((uint8_t*)ctx->root + offset);
			assert(mb->mb_size != 0);
			log_verbose("adding memblock at offset 0x%llx", offset);
			_addmemblock(ctx, mb);
		}
		ctx->data = _ptr(ctx, ctx->root->data);
		ctx->cache = _ptr(ctx, ctx->root->cache);
		log_verbose("data=%p cache=%p", ctx->data, ctx->cache);
		// Probably don't want to run the GC here...
		//db_gc(ctx, NULL);
	}
	ctx->sync = 1;
	return ctx;
}

void dbfile_addsize(pgctx_t *ctx)
{
	uint64_t chunksize;
	memblock_t *mb;
	int ret, n;

	chunksize = ctx->root->meta.chunksize;
	log_debug("Resizing mmfile +%d bytes", chunksize);
	ret = mm_resize(&ctx->mm, ctx->mm.size + chunksize);
	if (ret != 0) {
		log_error("Resize failed"); abort();
	}
	n = ctx->mm.nmap-1;
	mb = ctx->mm.map[n].ptr;
#ifdef WIN32
	memset(mb, 0, chunksize);
#endif
	mb->mb_offset = ctx->mm.map[n].offset;
	mb->mb_size = chunksize;
	mb->pg_size = 4096;
	mb->pg_count = chunksize / 4096;
	_addmemblock(ctx, mb);
}

void dbfile_resize(pgctx_t *ctx)
{
	uint64_t size, oldsize;
	uint64_t offset;
	memblock_t *mb;
	int ret, n;

	log_debug("Resizing mmfile");
	_dblockop(ctx, MLCK_WR, ctx->root->resize_lock);
	oldsize = ctx->mm.size;
	size = mm_size(&ctx->mm);
	if (size != oldsize) {
		// If the file has already been resized, just adjust our mapping
		ret = mm_resize(&ctx->mm, size);
		if (ret != 0) {
			log_error("Resize failed"); abort();
		}
		n = ctx->mm.nmap - 1;
		mb = ctx->mm.map[n].ptr;
		offset = mb->mb_offset;
	       	while(offset<ctx->mm.size) {
			_addmemblock(ctx, mb);
			offset+=mb->mb_size;
			mb = (memblock_t*)((uint8_t*)mb + mb->mb_size);
		}
	} else {
		// Otherwise, add size to the file
		dbfile_addsize(ctx);
	}
	_dblockop(ctx, MLCK_UN, ctx->root->resize_lock);
}

void dbfile_close(pgctx_t *ctx)
{
	int i;
	for(i=0; i<NR_DB_CONTEXT; i++) {
		if (dbctx[i] == ctx) {
			dbctx[i] = NULL;
		}
	}
        mm_close(&ctx->mm);
}

void dbfile_sync(pgctx_t *ctx)
{
	//uint32_t t0, t1;
	//t0 = utime_now();
	mm_sync(&ctx->mm);
	//t1 = utime_now();
	//log_debug("sync took %dus", t1-t0);
}


int _dbmemlock(pgctx_t *ctx, memblock_t *mb, uint32_t op)
{
#ifndef PMEM_LOCKFREE
	uint64_t chunk;
	uint64_t locksz;

	chunk = mb->mb_offset + mb->pg_size;
	locksz = mb->pg_count * sizeof(uint32_t);
	return mm_lock(&ctx->mm, op, chunk, locksz);
#endif
	return 0;
}

void dblock(pgctx_t *ctx)
{
	_dblockop(ctx, MLCK_RD, ctx->root->lock);
}

void dbunlock(pgctx_t *ctx)
{
	_dblockop(ctx, MLCK_UN, ctx->root->lock);
}

void *_dballoc(pgctx_t *ctx, int size, int lock)
{
	void *ret;
	memblock_t *mb;
	int n;
	int tries;
	int szcl;

	if (size > 2048) {
		szcl = DBMEM_BUCKET_PAGE;
	} else {
		size = (size + 15) & ~15;
		szcl = ntz(pow2(size))-4;
	}
	for(tries=0; tries<3; tries++) {
		n=ctx->last_mb[szcl];
		do {
			mb = ctx->mb[n];
			// By convention, lock the page pool when doing alloc or free
			if (lock) _dbmemlock(ctx, mb, MLCK_WR);
			ret = palloc(mb, size);
			if (lock) _dbmemlock(ctx, mb, MLCK_UN);
			if (ret) {
				memset(ret, 0, psize(mb, ret));
				ctx->last_mb[szcl] = n;
				return ret;
			} else {
				//log_debug("malloc failed in mb=%p, offset=0x%llx", mb, mb->mb_offset);
#if 0
				if (ctx->nr_mb>4) {
					pongo_abort("dballoc");
					return NULL;
				}
#endif
			}
			n = (n+1) % ctx->nr_mb;
		} while(n != ctx->last_mb[szcl]);
		//log_debug("malloc of %d bytes failed (tries=%d, nr_mb=%d, tid=%08lx)", size, tries, ctx->nr_mb, GetCurrentThreadId());
		dbfile_resize(ctx);
	}
	return NULL;
}

void *dballoc(pgctx_t *ctx, int size)
{
	return _dballoc(ctx, size, 1);
}

void _dbfree(pgctx_t *ctx, void *addr, int lock)
{
	memblock_t *mb;

	if (!addr) return;
        mb = __mb_ptr(ctx, addr);
	if (lock) {
		// By convention, lock the page pool when doing alloc or free
		_dbmemlock(ctx, mb, MLCK_WR);
	}
	pfree(mb, addr);
	if (lock) {
		_dbmemlock(ctx, mb, MLCK_UN);
	}
}

void dbfree(pgctx_t *ctx, void *addr)
{
#ifndef MARKING_GC
	// This competes with the GC and the marking GC may see
	// double frees.  It would be ok to call free if we took
	// the GC lock.
	_dbfree(ctx, addr, 1);
#endif
}

int dbsize(pgctx_t *ctx, void *addr)
{
	memblock_t *mb = __mb_ptr(ctx, addr);
	return psize(mb, addr);
}


static void gc_count(void *addr, int size, void *user)
{
	gccount_t *c = (gccount_t*)user;
	c->size += size;
	c->num++;
}

#ifdef SCANNING_GC
/***********************************************************************
 * Scanning Garbage collector:
 *
 * The garbage collector is a simple 2-pass collector:
 * 1. Collect all known allocations into a chained-bucket hash table
 * 2. Walk the data structures from the known roots and eliminate
 *    referenced pointers.
 *
 * Anything that remains in the hashtable is freed.
 *
 * The garbage collector can't examine thread (or process) stacks,
 * so, the collector can only run when no one is in the database.
***********************************************************************/
static void gc_collect(void *addr, int size, void *user)
{
	gchash_t *hash = (gchash_t*)user;
	gcbucket_t *bucket;
	uint32_t hofs = ((uint32_t)addr >> 4) % hash->hash_sz;
	int len = 0;

	bucket = hash->bucket[hofs];
	if (!bucket || bucket->len % GC_BUCKET_LEN == 0) {
		if (bucket) len = bucket->len;
		bucket = realloc(bucket, sizeof(*bucket)+(len+GC_BUCKET_LEN)*sizeof(bucket->ptr[0]));
		bucket->len = len;
		hash->bucket[hofs] = bucket;
	}

	//log_verbose("Collecting %p", addr);
	bucket->ptr[bucket->len++] = addr;
}

static void gc_keep(gchash_t *hash, void *addr)
{
	void *not_found = addr;
	gcbucket_t *bucket;
	uint32_t hofs = ((uint32_t)addr >> 4) % hash->hash_sz;
	int i;

	if (!addr)
		return;

	//log_verbose("Keeping %p", addr);
	bucket = hash->bucket[hofs];
	assert(bucket != NULL);
	for(i=0; i<bucket->len; i++) {
		if (bucket->ptr[i] == addr) {
			bucket->ptr[i] = NULL;
			return;
		}
	}
	assert(addr == not_found);
}

static int gc_free(pgctx_t *ctx, gchash_t *hash)
{
	gcbucket_t *bucket;
	void *ptr;
	int i, j, total=0;

	for(i=0; i<hash->hash_sz; i++) {
		bucket = hash->bucket[i];
		if (bucket) {
			for(j=0; j<bucket->len; j++) {
				ptr = bucket->ptr[j];
				if (ptr) {
					//log_verbose("GC freeing %p", ptr);
					dbcache_del(ctx, ptr);
					_dbfree(ctx, ptr, 0);
					total++;
				}
			}
			free(bucket);
		}
	}
	free(hash);
	return total;
}

static void gc_walk(pgctx_t *ctx, gchash_t *hash, dbtype_t *root)
{
	int i;
	_list_t *list;
	_obj_t *obj;
	if (!root)
		return;

	gc_keep(hash, root);
	switch(root->type) {
		case List:
			list = _ptr(ctx, root->list);
			gc_keep(hash, list);
			for(i=0; i<list->len; i++) 
				gc_walk(ctx, hash, _ptr(ctx, list->item[i]));
			break;
		case Object:
			obj = _ptr(ctx, root->obj);
			gc_keep(hash, obj);
			for(i=0; i<obj->len; i++) {
				gc_keep(hash, _ptr(ctx, obj->item[i].key));
				gc_walk(ctx, hash, _ptr(ctx, obj->item[i].value));
			}
			break;
		default:
			// Nothing to do
			break;

	}
}

int _db_gc(pgctx_t *ctx, gcstats_t *stats)
{
	int i, num;
	int64_t t0, t1, t2, t3, t4, t5;
	uint64_t offset;
	memblock_t *mb;
	_list_t *clist, *blist;
	dblist_t *bucket;
	gchash_t *hash = malloc(GC_HASH_MALLOC_SZ(GC_HASH_SZ));
	memset(hash, 0, GC_HASH_MALLOC_SZ(GC_HASH_SZ));
	hash->hash_sz = GC_HASH_SZ;

	t0 = utime_now();
	for(i=0; i<ctx->nr_mb; i++) {
		mb = ctx->mb[i];
	        if (stats) 
	        	pmem_foreach(mb, gc_count, &stats->before);
		// Collect all known allocations
		pmem_foreach(mb, gc_collect, hash);
	}
	t1 = utime_now();

	// Eliminate the const booleans and small integer pool
	gc_keep(hash, _ptr(ctx, ctx->root->booleans[0]));
	gc_keep(hash, _ptr(ctx, ctx->root->booleans[1]));
#if 0
	for(i=0; i<NR_SMALL_INTS; i++)
		gc_keep(hash, _ptr(ctx, ctx->root->integers+i*16));
#endif

	if (ctx->cache) {
		gc_keep(hash, ctx->cache);
#if 0
		clist = _ptr(ctx, ctx->cache->list);
		gc_keep(hash, clist);
		for(i=0; i<clist->len; i++) {
			bucket = _ptr(ctx, clist->item[i]);
			blist = _ptr(ctx, bucket->list);
			gc_keep(hash, bucket); gc_keep(hash, blist);
		}
#endif
	}
	t2 = utime_now();

	// Eliminate references that have parents that extend back to
	// the root "data" objects
	gc_walk(ctx, hash, ctx->data);
	t3 = utime_now();
	// Free everything that remains
	num = gc_free(ctx, hash);
	t4 = utime_now();
        if (stats) {
		for(i=0; i<ctx->nr_mb; i++) {
			mb = ctx->mb[i];
	        	pmem_foreach(mb, gc_count, &stats->after);
		}
	        log_debug("Memory utilization before gc=%d/%d, after=%d/%d",
                                stats->before.num, stats->before.size,
                                stats->after.num, stats->after.size);
        }
	t5 = utime_now();
	log_debug("GC timing:");
	log_debug("  Initial collection: %lldus", t1-t0);
	log_debug("     Keep const pool: %lldus", t2-t1);
	log_debug("                walk: %lldus", t3-t2);
	log_debug("                free: %lldus", t4-t3);
	log_debug("         final stats: %lldus", t5-t4);
	log_debug("               total: %lldus", t5-t0);
	return num;
}

int db_gc(pgctx_t *ctx, gcstats_t *stats)
{
	int num;
	_dblockop(ctx, MLCK_WR, ctx->root->lock);
	num = _db_gc(ctx, stats);
	_dblockop(ctx, MLCK_UN, ctx->root->lock);
	return num;
}
#endif

#ifdef MARKING_GC
static void gc_keep(pgctx_t *ctx, void *addr)
{
	memblock_t *mb;
	if (!addr)
		return;
	mb = __mb_ptr(ctx, addr);
	//printf("keeping type %02x at %p (%d bytes)\n", *(uint32_t*)addr, addr, psize(mb, addr));
	pmem_gc_keep(mb, addr);
}

static void gc_walk_cache(pgctx_t *ctx, dbtype_t *node)
{
	if (!node) return;
	gc_keep(ctx, node);
	gc_walk_cache(ctx, _ptr(ctx, node->left));
	gc_walk_cache(ctx, _ptr(ctx, node->right));
}

static void gc_walk(pgctx_t *ctx, dbtype_t *root)
{
	int i;
	_list_t *list;
	_obj_t *obj;
	if (!root)
		return;

	gc_keep(ctx, root);
	switch(root->type) {
		case List:
			list = _ptr(ctx, root->list);
			gc_keep(ctx, list);
			for(i=0; i<list->len; i++) 
				gc_walk(ctx, _ptr(ctx, list->item[i]));
			break;
		case Object:
			obj = _ptr(ctx, root->obj);
			gc_keep(ctx, obj);
			for(i=0; i<obj->len; i++) {
				gc_walk(ctx, _ptr(ctx, obj->item[i].key));
				gc_walk(ctx, _ptr(ctx, obj->item[i].value));
			}
			break;
		case Collection:
			gc_walk(ctx, _ptr(ctx, root->obj));
			break;
		case Cache:
			gc_walk_cache(ctx, _ptr(ctx, root->cache));
			break;
		case _BonsaiNode:
			gc_walk(ctx, _ptr(ctx, root->left));
			gc_walk(ctx, _ptr(ctx, root->key));
			gc_walk(ctx, _ptr(ctx, root->value));
			gc_walk(ctx, _ptr(ctx, root->right));
			break;
		default:
			// Nothing to do
			break;

	}
}

int _db_gc(pgctx_t *ctx, gcstats_t *stats)
{
	int i;
	int64_t t0, t1, t2, t3, t4, t5;
	memblock_t *mb;

	t0 = utime_now();
	for(i=0; i<ctx->nr_mb; i++) {
		mb = ctx->mb[i];
		// Lock the database during the mark phase
		// FIXME: It may not be necessary to lock at all during the
		// mark phase:
		//   We only set the PM_GC_MARK bit on fully allocated pages
		//   and we make a copy of the suballoc bitmaps.
		//
		//   If a page sized alloc happens, it will happen to a page
		//   we won't touch.  If a free happens, we wont care.
		//
		//   If additional suballocs happen, it wont affect our copy
		//   of the bitmap.
		//
		//   We do however have to do a lock/unlock after this to
		//   insure that anybody who was in the database is out
		//   by the time we go to the next phase.  That will prevent
		//   us from freeing memory that wasn't referenced by one
		//   of the root structures when we did the mark.
		//_dbmemlock(ctx, mb, MLCK_WR);
		pmem_gc_mark(mb);
		//_dbmemlock(ctx, mb, MLCK_UN);
	        if (stats)  {
			pmem_foreach(ctx->mb[i], gc_count, &stats->before);
		}
	}
	t1 = utime_now();

	// Synchronize here.  All this does is make sure anyone who was
	// in the database during the mark phase is out before we do the
	// walk phase.
	_dblockop(ctx, MLCK_WR, ctx->root->lock);
	_dblockop(ctx, MLCK_UN, ctx->root->lock);

	// Eliminate the const booleans
	gc_keep(ctx, _ptr(ctx, ctx->root->booleans[0]));
	gc_keep(ctx, _ptr(ctx, ctx->root->booleans[1]));
	gc_keep(ctx, _ptr(ctx, ctx->root->meta.id));

	t2 = utime_now();
	if (ctx->cache) {
		gc_walk(ctx, ctx->cache);
	}
	t3 = utime_now();

	// Eliminate references that have parents that extend back to
	// the root "data" objects
	gc_walk(ctx, ctx->data);

	// Also any references owned by all currently running processes
	gc_walk(ctx, _ptr(ctx, ctx->root->pidcache));
	t4 = utime_now();
	// Free everything that remains
	for(i=0; i<ctx->nr_mb; i++) {
		mb = ctx->mb[i];
		_dbmemlock(ctx, mb, MLCK_WR);
		pmem_gc_free(mb, (gcfreecb_t)dbcache_del, ctx);
		_dbmemlock(ctx, mb, MLCK_UN);
	        if (stats)  {
			pmem_foreach(mb, gc_count, &stats->after);
		}
	}
	t5 = utime_now();
	log_debug("GC timing:");
	log_debug("  mark: %lldus", t1-t0);
	log_debug("  sync: %lldus", t2-t1);
	log_debug(" cache: %lldus", t3-t2);
	log_debug("  walk: %lldus", t4-t3);
	log_debug("  free: %lldus", t5-t4);
	log_debug(" total: %lldus", t5-t0);
	return 0;
}

int db_gc(pgctx_t *ctx, gcstats_t *stats)
{
	int num;
	_dblockop(ctx, MLCK_WR, ctx->root->gc);
	num = _db_gc(ctx, stats);
	_dblockop(ctx, MLCK_UN, ctx->root->gc);
	return num;
}
#endif
