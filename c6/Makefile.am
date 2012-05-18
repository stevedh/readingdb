
SUBDIRS = hashtable pbuf

bin_PROGRAMS = reading-server

# reading_server_LDFLAGS = -lz
reading_server_LDADD = hashtable/src/libhashtable.a
reading_server_CFLAGS = -Ihashtable/src 
reading_server_SOURCES = util.c util-db.c reading-server.c logging.c stats-sock.c \
	rpc.c pbuf/rdb.pb-c.c compress.c commands.c
noinst_HEADERS = config.h  logging.h  readingdb.h  readingdb_py.h  \
	rpc.h  stats.h  util.h pbuf/rdb.proto commands.h

# noinst_PROGRAMS = compress
# compress_SOURCES = compress.c pbuf/rdb.pb-c.c rpc.c util.c logging.c
# compress_LDFLAGS = -lprotobuf-c -lz

install-data-local:
	mkdir -p $(DESTDIR)/$(localstatedir)/lib/readingdb
	mkdir -p $(DESTDIR)/$(sysconfdir)/monit/conf.d
	sh mkmonit.sh $(localstatedir) $(bindir) > $(DESTDIR)/$(sysconfdir)/monit/conf.d/readingdb
