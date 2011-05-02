
GPROF_FLAGS=-pg
GCOV_FLAGS=-fprofile-arcs -ftest-coverage

CFLAGS+=-Wall -Ihashtable/src 
LDFLAGS+=-ldb -lrt -lpthread hashtable/src/libhashtable.a 
TARGET=reading-server2


module: readingdb.i readingdb_wrap.c
	python setup.py build_ext --inplace

readingdb_wrap.c: readingdb.i ../c/readingdb.h readingdb_py.h
	swig -python -keyword -Wall readingdb.i

clean:
	rm _readingdb.so
	python setup.py clean

distclean: clean
	rm -f readingdb_wrap.c
