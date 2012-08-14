# Pongo Makefile

SRCS=pongogc.c

OBJS = $(SRCS:.c=.o)

CFLAGS=-fms-extensions -g3 -O2 -Wall
LIBS=-lm -luuid -lrt
INCLUDE=-Iinclude 
CC=gcc
LD=gcc

.c.o:
	$(CC) $(CFLAGS) $(INCLUDE) -c $< -o $@

pongogc: $(OBJS) lib/libpongo.a
	$(LD) -o $@ $< lib/libpongo.a $(LIBS)

all: pongogc

depend:
	$(CC) -E -MM $(INCLUDE) $(ALLSRC) > .depend

clean:
	find . -name "*.[oas]" | xargs rm
