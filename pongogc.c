#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pongo/dbmem.h>
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
    int interval = 5;
    char *dbfile = NULL;
    pgctx_t *ctx;

    for(i=1; i<argc; i++) {
        if (!strcmp(argv[i], "-i")) {
            interval = strtol(argv[++i], 0, 0);
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
    for(;;) {
        db_gc(ctx, NULL);
        sleep(interval);
    }
    return 0;
}

/*
 * vim: ts=4 sts=4 sw=4 expandtab:
 */
