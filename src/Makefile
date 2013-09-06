
LUADIR = /usr/local/include/luajit-2.0

COPT = -O2 -fPIC

CWARNS = -Wall

CFLAGS = $(CWARNS) $(COPT) -I$(LUADIR) -I./uv/include -L./uv

OS_NAME=$(shell uname -s)
MH_NAME=$(shell uname -m)

LDFLAGS=-luv -lm -ldl -lpthread

AR = ar

ifeq ($(OS_NAME), Darwin)
LDFLAGS+=-bundle -undefined dynamic_lookup -framework CoreServices
ifeq ($(MH_NAME), x86_64)
endif
else
LDFLAGS+=-shared -lrt
endif

SRCS := ray.c
OBJS := $(patsubst %.c,%.o,$(SRCS))

LIBS := ./uv/libuv.a

all: deps $(OBJS) ../ray.so

../ray.so: $(OBJS)
	$(CC) $(CFLAGS) $(COPT) $(LIBS) ray.c -o ../ray.so $(LDFLAGS)

$(OBJS):
	$(CC) -c $(CFLAGS) $(SRCS)

./uv/configure:
	cd ./uv && sh autogen.sh

./uv/Makefile: ./uv/configure
	cd ./uv && sh configure

./uv/libuv.a: ./uv/Makefile

deps: $(LIBS)
	$(MAKE) CFLAGS="-fPIC" -C ./uv
	cp ./uv/.libs/libuv.a ./uv/

clean:
	rm -f *.o *.so

realclean: clean
	$(MAKE) -C ./uv clean

.PHONY: all clean realclean

