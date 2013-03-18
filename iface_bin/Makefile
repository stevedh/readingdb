
CFLAGS+=-Wall
LDFLAGS+=

EXTRA_DIST=setup.py readingdb.i readingdb_py.h readingdb.c
SWIG_OUTPUTS = readingdb_wrap.c readingdb.py

PROJECT=readingdb-python
VERSION=0.6.0-3

all-local: module

install: module
	python setup.py install

prep: readingdb.i $(SWIG_OUTPUTS) ../c6/pbuf/rdb.pb-c.c
	mkdir -p c6/
	cp -r ../c6/pbuf/ c6/pbuf
	cp ../c6/readingdb.h ../c6/rpc.c ../c6/rpc.h ../c6/commands.h c6

module: prep
	python setup.py build_ext --inplace

dist: module
	python setup.py sdist --formats gztar 

builddeb: distclean prep
#	python setup.py sdist $(COMPILE) --dist-dir=../ --prune
	mkdir ../$(PROJECT)-$(VERSION)
	cp -r * ../$(PROJECT)-$(VERSION)
	rm -rf ../$(PROJECT)-$(VERSION)/dist
	tar zcvf ../$(PROJECT)_$(VERSION).orig.tar.gz ../$(PROJECT)-$(VERSION)
	rm -rf ../$(PROJECT)-$(VERSION)
	dpkg-buildpackage -i -I -rfakeroot -uc -us -S

$(SWIG_OUTPUTS): readingdb.i ../c6/readingdb.h readingdb_py.h
	swig -python -keyword -Wall readingdb.i

../c6/pbuf/rdb.pb-c.c: ../c6/pbuf/rdb.proto
	cd ../c6/pbuf/ && make

clean:
	-rm -rf ../$(PROJECT)_$(VERSION).orig.tar.gz ../$(PROJECT)-$(VERSION)
	-rm -f _readingdb.so *.pyc 
	-python setup.py clean
	-rm -rf c6

distclean: clean
	-rm -f  $(SWIG_OUTPUTS)
	-rm -rf dist
