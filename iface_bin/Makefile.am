
CFLAGS+=-Wall
LDFLAGS+=

EXTRA_DIST=setup.py readingdb.i readingdb_py.h readingdb.c
SWIG_OUTPUTS = readingdb_wrap.c readingdb.py

all-local: module

install-exec-local: module
	python setup.py install --prefix $(prefix)

module: readingdb.i $(SWIG_OUTPUTS) ../c6/pbuf/rdb.pb-c.c
	mkdir -p c6	
	cp -r ../c6/pbuf/ c6
	cp ../c6/readingdb.h ../c6/rpc.c ../c6/rpc.h ../c6/commands.h c6
	python setup.py build_ext --inplace

dist: module
	python setup.py sdist --formats gztar 

$(SWIG_OUTPUTS): readingdb.i ../c6/readingdb.h readingdb_py.h
	swig -python -keyword -Wall readingdb.i

../c6/pbuf/rdb.pb-c.c: ../c6/pbuf/rdb.proto
	cd ../c6/pbuf/ && make

clean-local:
	-rm -f _readingdb.so *.pyc
	-python setup.py clean
	-rm -rf c6

distclean-local: clean
	-rm -f  $(SWIG_OUTPUTS)
