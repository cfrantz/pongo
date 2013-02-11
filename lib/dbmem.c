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

pgctx_t *dbctx[NR_DB_CONTEXT];
int __dblocked;

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


void dbmem_info(pgctx_t *ctx)
{
	pmem_print_mem(&ctx->mm, _ptr(ctx, ctx->root->heap));
}

#ifdef WANT_UUID_TYPE
dbtype_t _newkey(pgctx_t *ctx, dbtype_t value)
{
	return dbuuid_new(ctx, NULL);
}
#endif

void *dbfile_more_mem(mmfile_t *mm, uint32_t *size)
{
	uint64_t chunksize;
	dbroot_t *root;
	int n, rc;
	void *ret = NULL;

	root = (dbroot_t*)mm->map[0].ptr;
	chunksize = root->meta.chunksize;
	mm_lock(mm, MLCK_WR, __offset(mm, &root->resize), sizeof(root->resize));
	if (mm->size == mm_size(mm)) {
		log_debug("Resizing mmfile +%d bytes", chunksize);
		rc = mm_resize(mm, mm->size + chunksize);
		if (rc != 0) {
			log_error("Resize failed"); abort();
		}
		n = mm->nmap-1;
		*size = mm->map_offset[n].size;
		ret = mm->map_offset[n].ptr;
	} else {
		chunksize = mm_size(mm);
		log_debug("Expanding mapping to 0x%x bytes", chunksize);
		mm_resize(mm, chunksize);
	}
	mm_lock(mm, MLCK_UN, __offset(mm, &root->resize), sizeof(root->resize));
	return ret;
}

pgctx_t *dbfile_open(const char *filename, uint32_t initsize)
{
	int ret, i;
	dbroot_t *r;
	pgctx_t *ctx;
	mempool_t *pool;
	memheap_t *heap;
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
	pmem_more_memory = dbfile_more_mem;
#ifdef WANT_UUID_TYPE
	ctx->newkey = _newkey;
#endif

	if (strcmp((char*)ctx->root->signature, DBROOT_SIG)) {
		r = ctx->root;
		strcpy((char*)r->signature, DBROOT_SIG);

		pool = pmem_pool_init((char*)ctx->mm.map[0].ptr+4096, ctx->mm.map[0].size-4096);
		heap = pmem_pool_alloc(pool, 4096);
		heap->nr_procheap = (4096-sizeof(*heap)) / sizeof(procheap_t);
		heap->mempool = _offset(ctx, pool);
		pmem_relist_pools(&ctx->mm, heap);
		r->heap = _offset(ctx, heap);

		// Create the root data dictionary
		ctx->data = dbcollection_new(ctx, 0);
		r->data = ctx->data;

		// Not sure about the atom cache...
		//ctx->cache = DBNULL;
		//ctx->cache = dbcache_new(ctx, 0, 0);
		//r->cache = ctx->cache;

		r->pidcache = dbcollection_new(ctx, 0);
		
		r->meta.chunksize = initsize;
		r->meta.id = dbstring_new(ctx, "_id", 3);
	} else {
		ctx->data = ctx->root->data;
		ctx->cache = ctx->root->cache;
		log_verbose("data=%" PRIx64 " cache=%" PRIx64, ctx->data.all, ctx->cache.all);
		// Probably don't want to run the GC here...
		//db_gc(ctx, NULL);
	}

	// Pidcache in "ctx" points to this process' pidcache.
	// The pidcache in root points to the global pidcache (container
	// of pidcaches)
	ctx->pidcache = DBNULL;
	ctx->sync = 1;
	return ctx;
}


void dbfile_close(pgctx_t *ctx)
{
	int i;
	for(i=0; i<NR_DB_CONTEXT; i++) {
		if (dbctx[i] == ctx) {
			dbctx[i] = NULL;
		}
	}
	pmem_retire(&ctx->mm, _ptr(ctx, ctx->root->heap), 0);
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
	__dblocked++;
}

void dbunlock(pgctx_t *ctx)
{
	__dblocked--;
	_dblockop(ctx, MLCK_UN, ctx->root->lock);
}

void *dballoc(pgctx_t *ctx, unsigned size)
{
	void *addr;
	addr = pmem_alloc(&ctx->mm, _ptr(ctx, ctx->root->heap), size);
	return addr;
}

/*
void dbfree(pgctx_t *ctx, void *addr)
{
	pmem_gc_suggest(addr);
}
*/


static void gc_keep(pgctx_t *ctx, void *addr)
{
	//memblock_t *mb = (memblock_t*)addr - 1;
	//printf("keeping type %02x at %p (%d bytes)\n", *(uint32_t*)addr, addr, psize(mb, addr));
	pmem_gc_keep(addr);
}

static void gc_walk_cache(pgctx_t *ctx, dbtype_t node)
{
	if (!node.all) return;
	node.ptr = dbptr(ctx, node);
	gc_keep(ctx, node.ptr);
	gc_walk_cache(ctx, node.ptr->left);
	gc_walk_cache(ctx, node.ptr->right);
}

static void printval(pgctx_t *ctx, dbtype_t val)
{
	char buf[100];
	printf("%s\n", dbprint(ctx, val, buf, 100));
}

static void gc_walk(pgctx_t *ctx, dbtype_t root)
{
	int i;
	_list_t *list;
	_obj_t *obj;
	if (root.all == 0 || !isPtr(root.type))
		return;

	root.ptr = dbptr(ctx, root);
	gc_keep(ctx, root.ptr);
	switch(root.ptr->type) {
		case List:
			list = dbptr(ctx, root.ptr->list);
			if (list) {
				gc_keep(ctx, list);
				for(i=0; i<list->len; i++) 
					gc_walk(ctx, list->item[i]);
			}
			break;
		case Object:
			obj = dbptr(ctx, root.ptr->obj);
			if (obj) {
				gc_keep(ctx, obj);
				for(i=0; i<obj->len; i++) {
					//printval(ctx, obj->item[i].key);
					gc_walk(ctx, obj->item[i].key);
					gc_walk(ctx, obj->item[i].value);
				}
			}
			break;
		case Collection:
		case MultiCollection:
			gc_walk(ctx, root.ptr->obj);
			break;
		case Cache:
			gc_walk_cache(ctx, root.ptr->cache);
			break;
		case _BonsaiNode:
			gc_walk(ctx, root.ptr->left);
			//printval(ctx, root.ptr->key);
			gc_walk(ctx, root.ptr->key);
			gc_walk(ctx, root.ptr->value);
			gc_walk(ctx, root.ptr->right);
			break;
		case _BonsaiMultiNode:
			gc_walk(ctx, root.ptr->left);
			gc_walk(ctx, root.ptr->key);
			for(i=0; i<root.ptr->nvalue; i++) {
				gc_walk(ctx, root.ptr->values[i]);
			}
			gc_walk(ctx, root.ptr->right);
			break;
		default:
			// Nothing to do
			break;

	}
}

int _db_gc(pgctx_t *ctx, gcstats_t *stats)
{
	int64_t t0, t1, t2, t3, t4, t5;
	memheap_t *heap = _ptr(ctx, ctx->root->heap);

	t0 = utime_now();
	pmem_gc_mark(&ctx->mm, heap, 0);
	t1 = utime_now();

	// Synchronize here.  All this does is make sure anyone who was
	// in the database during the mark phase is out before we do the
	// walk phase.
	_dblockop(ctx, MLCK_WR, ctx->root->lock);
	_dblockop(ctx, MLCK_UN, ctx->root->lock);

	// Eliminate the structures used by the memory subsystem itself
	gc_keep(ctx, heap);
	gc_keep(ctx, _ptr(ctx, heap->pool));

	// Eliminated references in the meta table
	if (isPtr(ctx->root->meta.id.type)) {
		gc_keep(ctx, dbptr(ctx, ctx->root->meta.id));
	}

	t2 = utime_now();
	gc_walk(ctx, ctx->cache);
	t3 = utime_now();

	// Eliminate references that have parents that extend back to
	// the root "data" objects
	gc_walk(ctx, ctx->data);

	// Also any references owned by all currently running processes
	gc_walk(ctx, ctx->root->pidcache);
	t4 = utime_now();
	// Free everything that remains
	//pmem_gc_free(&ctx->mm, heap, 0, (gcfreecb_t)dbcache_del, ctx);
	pmem_gc_free(&ctx->mm, heap, 0, NULL, ctx);

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

int _db_gc_fast(pgctx_t *ctx)
{
	memheap_t *heap = _ptr(ctx, ctx->root->heap);

	// Synchronize here.  All this does is make sure anyone who was
	// in the database using the blocks that were suggested to be
	// freed is now out of the database and the blocks can be
	// safely freed.
	pmem_gc_mark(&ctx->mm, heap, 1);
	_dblockop(ctx, MLCK_WR, ctx->root->lock);
	_dblockop(ctx, MLCK_UN, ctx->root->lock);
	pmem_gc_free(&ctx->mm, heap, 1, NULL, ctx);
	//pmem_gc_free(&ctx->mm, heap, 1, (gcfreecb_t)dbcache_del, ctx);
	return 0;
}

int db_gc(pgctx_t *ctx, int complete, gcstats_t *stats)
{
	int num;
	_dblockop(ctx, MLCK_WR, ctx->root->gc);
	if (complete) {
		num = _db_gc(ctx, stats);
	} else {
		num = _db_gc_fast(ctx);
	}
	_dblockop(ctx, MLCK_UN, ctx->root->gc);
	return num;
}
