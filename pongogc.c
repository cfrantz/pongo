#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pongo/dbmem.h>
#include <pongo/json.h>
#include <pongo/misc.h>
#include <pongo/log.h>

int
usage(const char *progname)
{
    printf("%s [-f dbfile] [-l seconds] [-s seconds] [-i] [-d]\n"
        "    PongoDB Garbage Collector:\n"
        "        -f: Database file on which to operate\n"
        "        -l: Long GC interval (full collection)\n"
        "        -s: Short GC interval (quick collections)\n"
        "        -i: Allocator/Heap info\n"
        "        -d: dump database as json to stdout\n",
        progname);
    return 1;
}

void
print_meminfo(pgctx_t *ctx)
{
    memheap_t *heap = _ptr(ctx, ctx->root->heap);
    pmem_print_mem(&ctx->mm, heap);
}

int
main(int argc, char *argv[])
{
    int i;
    unsigned long_interval = 60000000;
    unsigned short_interval = 250000;
    char *dbfile = NULL;
    int64_t t0, t1;
    pgctx_t *ctx;
    int info = 0, dump = 0;

    for(i=1; i<argc; i++) {
        if (!strcmp(argv[i], "-l")) {
            long_interval = atof(argv[++i]) * 1e6;
        } else if (!strcmp(argv[i], "-s")) {
            short_interval = atof(argv[++i]) * 1e6;
        } else if (!strcmp(argv[i], "-f")) {
            dbfile = argv[++i];
        } else if (!strcmp(argv[i], "-i")) {
            info = 1;
        } else if (!strcmp(argv[i], "-d")) {
            dump = 1;
        } else {
            return usage(argv[0]);
        }
    }
    if (!dbfile)
        return usage(argv[0]);

    printf("PongoGC: file=%s\n", dbfile);
    log_init(NULL, LOG_DEBUG);
    ctx = dbfile_open(dbfile, 0);
    if (info) {
        print_meminfo(ctx);
        return 0;
    }
    if (dump) {
        json_dump(ctx, ctx->root->data, stdout);
        return 0;
    }

    printf("  short_interval=%uus\n", short_interval);
    printf("   long_interval=%uus\n", long_interval);
    db_gc(ctx, 1, NULL);
    t0 = utime_now();
    for(;;) {
        usleep(short_interval);
        t1 = utime_now();
        if (t1-t0 < long_interval) {
            db_gc(ctx, 0, NULL);
        } else {
            db_gc(ctx, 1, NULL);
            t0 = utime_now();
        }
    }
    return 0;
}

/*
 * vim: ts=4 sts=4 sw=4 expandtab:
 */
