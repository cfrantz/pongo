# YAJL Makefile
#

SRCS= yajl_alloc.c yajl_buf.c yajl.c yajl_encode.c yajl_gen.c \
      yajl_lex.c yajl_parser.c yajl_tree.c yajl_version.c
OBJS = $(SRCS:.c=.o)

CFLAGS=-O2 -g3
LIBS=
INCLUDE=-I../include 
CC=gcc
LD=gcc

.c.o:
	$(CC) $(CFLAGS) $(INCLUDE) -c $< -o $@

libyajl.a: $(OBJS)
	$(AR) rcs libyajl.a $(OBJS)

all: libyajl.a

depend:
	$(CC) -E -MM $(INCLUDE) $(ALLSRC) > .depend

clean:
	find . -name "*.[oa]" | xargs rm

