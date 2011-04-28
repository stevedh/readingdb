
GPROF_FLAGS=-pg
GCOV_FLAGS=-fprofile-arcs -ftest-coverage

CFLAGS+=-Wall -Ihashtable/src -O3 -I/usr/local/BerkeleyDB.4.8/include
LDFLAGS+=-ldb -lrt -lpthread hashtable/src/libhashtable.a -L/usr/local/BerkeleyDB.4.8/lib
TARGET=reading-server3

all: $(TARGET) # module dump-db

debug: $(TARGET) gprof-helper.so

gprof-helper.so: gprof-helper.c
	gcc -shared -fPIC gprof-helper.c -o gprof-helper.so -lpthread -ldl

module: readingdb2.i readingdb.c readingdb.h util.c readingdb_py.h
	swig -python readingdb2.i
	python setup.py build_ext --inplace

clean:
	rm -rf readingdb_wrap.c _readingdb.so *.o *.pyc build/ $(TARGET) reading-client dump-db

test:
	gcc -o test-bdb test-bdb.c -ldb
	gcc -o test-query test-query.c -ldb


SOURCES=util.c util-db.c reading-server.c logging.c stats-sock.c
OBJECTS=$(SOURCES:.c=.o)

$(TARGET): $(OBJECTS)
	$(CC) -o $@ $(OBJECTS) $(LDFLAGS)

DB_DUMP_SOURCES=dump-db.c util.c util-db.c logging.c
DB_DUMP_OBJECTS=$(DB_DUMP_SOURCES:.c=.o)
dump-db: $(DB_DUMP_OBJECTS)
	$(CC) -o $@ $(DB_DUMP_OBJECTS) $(LDFLAGS)

reading-client: util.o reading-client.o readingdb.h
	gcc -o reading-client reading-client.o util.o $(LDFLAGS)

%.o: %.c
	$(CC) -o $@ $< -c $(CFLAGS)

# this from the make manual... thanks guys
include $(subst .c,.d,$(SOURCES))

%.d: %.c
	$(CC) -M $(CFLAGS) $< > $@.$$$$;                    \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$
