
CFLAGS+=-Wall
LDFLAGS+=


module: readingdb.i readingdb_wrap.c ../c6/pbuf/rdb.pb-c.c
	python setup.py build_ext --inplace

readingdb_wrap.c: readingdb.i ../c6/readingdb.h readingdb_py.h
	swig -python -keyword -Wall readingdb.i

../c6/pbuf/rdb.pb-c.c: ../c6/pbuf/rdb.proto
	cd ../c6/pbuf/ && make

clean:
	rm _readingdb.so
	python setup.py clean

distclean: clean
	rm -f readingdb_wrap.c
