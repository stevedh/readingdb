
CFLAGS+=-Wall
LDFLAGS+=

EXTRA_DIST=setup.py readingdb.i readingdb_py.h readingdb.c

all-local: module

install-exec-local: module
	python setup.py install --prefix $(prefix)

module: readingdb.i readingdb_wrap.c ../c6/pbuf/rdb.pb-c.c
	python setup.py build_ext --inplace

readingdb_wrap.c: readingdb.i ../c6/readingdb.h readingdb_py.h
	swig -python -keyword -Wall readingdb.i

../c6/pbuf/rdb.pb-c.c: ../c6/pbuf/rdb.proto
	cd ../c6/pbuf/ && make

clean-local:
	-rm -f _readingdb.so *.pyc
	-python setup.py clean

distclean-local: clean
	-rm -f readingdb_wrap.c
