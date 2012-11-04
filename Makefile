# Pongo Makefile

SRCS=pongogc.c

OBJS = $(SRCS:.c=.o)

CFLAGS=-fms-extensions -g3 -O2 -Wall
LIBS=-lm -luuid -lrt #--coverage
INCLUDE=-Iinclude 
CC=gcc
LD=gcc

.c.o:
	$(CC) $(CFLAGS) $(INCLUDE) -c $< -o $@

pongogc: $(OBJS) lib/libpongo.a
	$(LD) -o $@ $< lib/libpongo.a yajl/libyajl.a $(LIBS)

struct-check: struct-check.c
	$(CC) $(CFLAGS) $(INCLUDE) -o struct-check $<

all: pongogc

depend:
	$(CC) -E -MM $(INCLUDE) $(ALLSRC) > .depend

clean:
	rm -f pongogc pongogc.o struct-check
