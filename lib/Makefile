# Pongo Makefile
#

SRCS=dbmem.c dbtypes.c log.c mmfile.c pmem.c misc.c json.c bonsai.c pidcache.c \
     container_list.c \
     container_obj.c \
     container_coll.c \
     container_cache.c \
     container_ops.c 

OBJS = $(SRCS:.c=.o)

COVERAGE=-fprofile-arcs -ftest-coverage
CFLAGS=-fms-extensions -g3 -O2 -Wall -DWANT_UUID_TYPE $(COVERAGE)
LIBS=-lc -luuid
INCLUDE=-I../include 
CC=gcc
LD=gcc

.c.o:
	$(CC) $(CFLAGS) $(INCLUDE) -c $< -o $@

libpongo.a: $(OBJS)
	$(AR) rcs libpongo.a $(OBJS)

all: libpongo.a

depend:
	$(CC) -E -MM $(INCLUDE) $(ALLSRC) > .depend

clean:
	find . -name "*.[oas]" | xargs rm
