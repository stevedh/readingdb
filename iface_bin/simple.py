
import readingdb as rdb
rdb.db_setup('localhost', 4242)
a = rdb.db_open('localhost')

rdb.db_add(a, 1, [(x, 0, x) for x in xrange(0, 100)])
print rdb.db_query(1, 0, 100, conn=a)
rdb.db_close(a)
