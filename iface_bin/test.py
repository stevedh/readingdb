
import sys
import time
import readingdb as rdb

end = 1304102690

db = rdb.db_open(host='localhost', port=4242)
# db = rdb.db_open()
rdb.db_substream(db, 0)

S1MAX = 1000 * 100
if len(sys.argv) == 1:
    print "%s [-a | -r | -n]" % sys.argv[0]
elif sys.argv[1] == '-a':
    # substream 1 has every bucket filled
    for i in range(0, 1000):
        data = [(x, x, x) for x in xrange(i * 100, i * 100 + 100)]
        rdb.db_add(db, 1, data)

    # substream 2 has points one hour apart
    for i in range(0, 10000):
        rdb.db_add(db, 2, [(i * 3600, 0, i * 3600)])
elif sys.argv[1] == '-r':
    # test that we read back what we wrote
    d = rdb.db_query(db, 1, 0, 10000)
    assert len(d) == 10000
    for i in xrange(0, 10000):
        assert d[i][0] == i
        assert d[i][1] == i

    d = rdb.db_query(db, 2, 0, 3600 * 10000)
    for i in xrange(0, 10000):
        assert d[i][0] == i * 3600
elif sys.argv[1] == '-n':
    # test that db_next and db_prev iterate correctly through the data
    for i in xrange(0, 10000):
        d = rdb.db_next(db, 1, i)
        assert d[0][0] == i+1
        d = rdb.db_prev(db, 1, i)
        if i == 0:
            assert len(d) == 0
        else:
            assert d[0][0] == i - 1
    for i in xrange(1, 100000):
        d = rdb.db_next(db, 2, i)
        assert d[0][0] == (i + 3600 - (i % 3600))
        d = rdb.db_prev(db, 2, i)
        prev =  i - 3600 + (3600 - (i % 3600))
        if i % 3600 == 0: prev -= 3600
        assert d[0][0] == prev
elif sys.argv[1] == '-s':
    for i in xrange(1, 2000):
        s = time.time()
        x = rdb.db_prev(db, i, int(time.time()), n=10)
        print len(x), (time.time() - s)
elif sys.argv[1] == '-l':

    for f in sys.argv[2:]:
        with open(f, 'r') as fp:
            add_vec = []
            streamid = int(f[f.rindex('.')+1:])
            for line in fp.readlines():
                parts = line.strip().split(',')
                print parts
                assert len(parts) == 2
                tuple = (int(parts[0]), 0, float(parts[1]))
                add_vec.append(tuple)
                if len(add_vec) == 100:
                    rdb.db_add(db, streamid, add_vec)
                    add_vec = []
else:
    print "invalid argument"

# for i in xrange(0, 1000):
#     x = rdb.db_next(db, 1, i)
#     assert x[0][0] == i+1

rdb.db_close(db)
