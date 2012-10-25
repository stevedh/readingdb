
all-local: rdb.pb-c.h

rdb.pb-c.h: rdb.proto
	protoc-c --c_out=. $<


clean-local:
	rm -f rdb.pb-c.*
