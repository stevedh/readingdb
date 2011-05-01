
GPROF_FLAGS=-pg
GCOV_FLAGS=-fprofile-arcs -ftest-coverage

CFLAGS+=-Wall -Ihashtable/src 
LDFLAGS+=-ldb -lrt -lpthread hashtable/src/libhashtable.a 
TARGET=reading-server2


module: readingdb.i readingdb.c ../c/readingdb.h readingdb_py.h
	swig -python readingdb.i
	python setup.py build_ext --inplace

