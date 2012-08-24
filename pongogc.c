#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pongo/dbmem.h>
#include <pongo/misc.h>
#include <pongo/log.h>

int
usage(const char *progname)
{
    printf("%s [-i interval] [-f dbfile]\n"
        "    PongoDB Garbage Collector:\n"
        "        -i: Set the interval between GC runs\n"
        "        -f: Database file on which to operate\n",
        progname);
    return 1;
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

    for(i=1; i<argc; i++) {
        if (!strcmp(argv[i], "-l")) {
            long_interval = atof(argv[++i]) * 1e6;
        } else if (!strcmp(argv[i], "-s")) {
            short_interval = atof(argv[++i]) * 1e6;
        } else if (!strcmp(argv[i], "-f")) {
            dbfile = argv[++i];
        } else {
            return usage(argv[0]);
        }
    }
    if (!dbfile)
        return usage(argv[0]);

    log_init(NULL, LOG_DEBUG);
    ctx = dbfile_open(dbfile, 0);
    t0 = utime_now();
    for(;;) {
        usleep(short_interval);
        t1 = utime_now();
        if (ctx->nr_mb != ctx->root->nr_mb) {
            dbfile_resize(ctx);
        }
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
