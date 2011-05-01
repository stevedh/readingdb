
GPROF_FLAGS=-pg
GCOV_FLAGS=-fprofile-arcs -ftest-coverage

CFLAGS+=-I/usr/local/BerkeleyDB.4.8/include
LDFLAGS+=-L/usr/local/BerkeleyDB.4.8/lib

CFLAGS+=-Wall -Ihashtable/src -g
LDFLAGS+=-ldb -lrt -lpthread -lprotobuf-c hashtable/src/libhashtable.a
TARGET=reading-server

all: $(TARGET) # module dump-db

clean:
	rm -rf readingdb_wrap.c _readingdb.so *.o *.pyc *.d build/ $(TARGET) 
	cd pbuf && make clean

SOURCES=util.c util-db.c reading-server.c logging.c stats-sock.c \
	rpc.c pbuf/rdb.pb-c.c
OBJECTS=$(SOURCES:.c=.o)

pbuf: FORCE
	cd pbuf && make

$(TARGET): $(OBJECTS)
	$(CC) -o $@ $(OBJECTS) $(LDFLAGS)

%.o: %.c
	$(CC) -o $@ $< -c $(CFLAGS)

# this from the make manual... thanks guys
include $(subst .c,.d,$(SOURCES))

%.d: %.c pbuf
	$(CC) -M $(CFLAGS) $< > $@.$$$$;                    \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

FORCE:

