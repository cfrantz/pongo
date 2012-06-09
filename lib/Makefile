# Pongo Makefile
#

SRCS=containers.c dbmem.c dbtypes.c log.c mmfile.c pmem.c misc.c \
     json.c
OBJS = $(SRCS:.c=.o)

CFLAGS=-fms-extensions -g3 -O2 -Wall
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
