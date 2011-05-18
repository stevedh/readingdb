
import time
import sys
sys.path.append('../readingdb')

import readingdb as rdb1
import readingdb2 as rdb2

def tic():
    return time.time()
def toc(v, p=True):
    delta = (time.time() - v)
    if p:
        print "%.3f" % delta
    return delta

IMPORT_START = 0 # int(time.time() - 3600 * 24 * 20)

if __name__ == '__main__':
    db1 = rdb1.db_open()
    db2 = rdb2.db_open()
    # 1222
    start = int(sys.argv[1])
    for stream in range(start, 5000):
        print "ADDING STREAM", stream
        vec = rdb1.db_query(db1, stream, IMPORT_START, 2**32-1)
        while len(vec) == 10000:
            t = tic()
            vec = rdb1.db_query(db1, stream, vec[-1][0] - 1, 2**32-1)
            vec = [(x[0], 0, x[1], 0, 0) for x in vec]
            print "received", len(vec),
            toc(t)
            t = tic()
            rdb2.db_add(db2, stream, vec)
            print "inserted", stream, 
            toc(t)
            # time.sleep(.5)
        vec = [(x[0], 0, x[1], 0, 0) for x in vec]
        rdb2.db_add(db2, stream, vec)
        rdb2.db_sync(db2)
    rdb1.db_close(db1)
    rdb2.db_close(db2)
