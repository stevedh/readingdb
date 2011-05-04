
GPROF_FLAGS=-pg
GCOV_FLAGS=-fprofile-arcs -ftest-coverage

CFLAGS+=-I/usr/local/BerkeleyDB.4.8/include -I/opt/local/include/db48
LDFLAGS+=-L/usr/local/BerkeleyDB.4.8/lib -L/opt/local/lib/db48

CFLAGS+=-Wall -Ihashtable/src -O2
LDFLAGS+=-ldb -lpthread -lprotobuf-c hashtable/src/libhashtable.a
TARGET=reading-server

all: 
	cd pbuf && make
	make $(TARGET) # module dump-db

clean:
	rm -rf readingdb_wrap.c _readingdb.so *.o *.pyc *.d build/ $(TARGET) 
	cd pbuf && make clean

SOURCES=util.c util-db.c reading-server.c logging.c stats-sock.c \
	rpc.c pbuf/rdb.pb-c.c
OBJECTS=$(SOURCES:.c=.o)

$(TARGET): $(OBJECTS)
	$(CC) -o $@ $(OBJECTS) $(LDFLAGS)

%.o: %.c
	$(CC) -o $@ $< -c $(CFLAGS)

# this from the make manual... thanks guys
include $(subst .c,.d,$(SOURCES))

%.d: %.c 
	$(CC) -M $(CFLAGS) $< > $@.$$$$;                    \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

FORCE:

