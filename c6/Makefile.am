
SUBDIRS = hashtable pbuf

bin_PROGRAMS = reading-server

reading_server_LDFLAGS = -ldb -lpthread -lprotobuf-c -lz
reading_server_LDADD = hashtable/src/libhashtable.a
reading_server_CFLAGS = -Ihashtable/src
reading_server_SOURCES = util.c util-db.c reading-server.c logging.c stats-sock.c \
	rpc.c pbuf/rdb.pb-c.c compress.c

# noinst_PROGRAMS = compress
# compress_SOURCES = compress.c pbuf/rdb.pb-c.c rpc.c util.c logging.c
# compress_LDFLAGS = -lprotobuf-c -lz
