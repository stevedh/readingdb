
import sys
import time
import readingdb as rdb
import _readingdb

import numpy as np

print "using readingdb", rdb.__file__
print _readingdb.__file__

end = 1304102690

rdb.db_setup('localhost', 4242)
db = rdb.db_open(host='localhost', port=4242)
# db = rdb.db_open()

def next(id, ref, n=1): 
    return rdb.db_next(id, ref, n=n, conn=db)[0].tolist()

def prev(id, ref, n=1, conn=db): 
    return rdb.db_prev(id, ref, n=n, conn=db)[0].tolist()

S1MAX = 1000 * 100
if len(sys.argv) == 1:
    print "%s [-a | -r | -n | -d | -c]" % sys.argv[0]
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
    d = rdb.db_query(1, 0, 10000)
    assert len(d) == 1
    d = d[0].tolist()
    assert len(d) == 10000
    
    for i in xrange(0, 10000):
        assert d[i][0] == i
        assert d[i][1] == i

    d = rdb.db_query(2, 0, 3600 * 10000)
    assert len(d) == 1
    d = d[0].tolist()
    print d[0:10]
    for i in xrange(0, 10000):
        assert d[i][0] == i * 3600
elif sys.argv[1] == '-d': 
    # test we can delete some data
    d = rdb.db_del(db, 1, 0, 10000)
    d = rdb.db_del(db, 2, 0, 1000000)
elif sys.argv[1] == '-n':
    # test that db_next and db_prev iterate correctly through the data
    for i in xrange(0, 10000):
        d = next(1, i)
        assert d[0][0] == i+1
        # print i
        d = prev(1, i)
        if i == 0:
            assert len(d) == 0
        else:
            assert d[0][0] == i - 1
        if not i % 100:
            print '.',
    # print "done with test 1"
    for i in xrange(1, 100000):
        d = next(2, i)
        assert d[0][0] == (i + 3600 - (i % 3600))
        d = prev(2, i)
        p =  i - 3600 + (3600 - (i % 3600))
        if i % 3600 == 0: p -= 3600
        assert d[0][0] == p
        if not i % 1000:
            print '.', 
elif sys.argv[1] == '-s':
    for i in xrange(1, 2000):
        s = time.time()
        x = prev(i, int(time.time()), n=10)
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
elif sys.argv[1] == '-m':
    d = rdb.db_query([1, 2], 0, 100000000000, limit=-1)
    d1, d2 = d
    assert np.shape(d1) == (1e5, 2)
    assert np.shape(d2) == (1e4, 2)
    assert np.sum(d1[:, 0] - np.arange(0, 1e5)) == 0
    assert np.sum(d1[:, 1] - np.arange(0, 1e5)) == 0
    assert np.sum(d2[:, 0] - np.arange(0, 3600 * 1e4, 3600)) == 0
    assert np.sum(d2[:, 1] - np.arange(0, 3600 * 1e4, 3600)) == 0
else:
    print "invalid argument"

# for i in xrange(0, 1000):
#     x = rdb.db_next(db, 1, i)
#     assert x[0][0] == i+1

rdb.db_close(db)
