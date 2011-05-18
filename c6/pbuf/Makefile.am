
all: rdb.pb-c.c

rdb.pb-c.c: rdb.proto
	protoc-c --c_out=. $<

