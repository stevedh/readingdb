
import re
import time
import sys
from optparse import OptionParser
import numpy as np

import readingdb as rdb4

def tic():
    return time.time()
def toc(v, p=True):
    delta = (time.time() - v)
    if p:
        print "%.3f" % delta
    return delta

IMPORT_START = int(time.time() - 3600 * 35)
def parse_netloc(loc):
    split = loc.rfind(':')
    if split > 0:
        return loc[:split], int(loc[split+1:])
    else:
        return loc, 4246

if __name__ == '__main__':
    parser = OptionParser(description="Import data from one readingdb to another",
                          usage="usage: %prog [options] db1 db2")
    parser.add_option('-s', '--substream', dest='substream', default='0',
                      help='which substream to import')
    parser.add_option('-a', '--ago', dest='ago', default='1',
                      help='how long ago to begin or end importing from (hours)')
    parser.add_option('-z', '--zero', dest='zero', default=False, action='store_true',
                      help='start import at time zero')
    parser.add_option('-b', '--begin-streamid', dest='startid', default='0',
                      help='what streamid to start with (default=0')
    parser.add_option('-m', '--max-streamid', dest='maxid', default='10000',
                      help='what streamid to stop at (default=10000')
    parser.add_option('-d', '--delay', dest='delay', default='0.1',
                      help='how long to wait between each query-insert (default=0.1s)')
    parser.add_option('-n', '--no-action', dest='noop', default=False, action='store_true',
                      help='don\'t actually insert the data')
    parser.add_option('-f', '--map-file', dest='mapfile', default=False,
                      help='import using a map file')
    opts, hosts = parser.parse_args()
    if len(hosts) != 2:
        parser.print_help()
        sys.exit(1)

    old_db = parse_netloc(hosts[0])
    new_db = parse_netloc(hosts[1])

    print "Importing data from %s:%i to %s:%i" % (old_db + new_db)
    print "substream: %i" % int(opts.substream)
    
    rdb4.db_setup(old_db[0], old_db[1])
    db0 = rdb4.db_open(host=old_db[0], port=old_db[1])
    db1 = rdb4.db_open(host=new_db[0], port=new_db[1])

    # rdb4.db_substream(db0, int(opts.substream))
    # rdb4.db_substream(db1, int(opts.substream))

    if not opts.zero:
        IMPORT_START = int(time.time()) - (int(opts.ago) * 3600)
        IMPORT_STOP = 2**32 - 10
    else:
        IMPORT_START = 1
        IMPORT_STOP = int(time.time()) - (int(opts.ago) * 3600)
    print "Importing from %i to %i" % (IMPORT_START, IMPORT_STOP)

    if not opts.mapfile:
        start = int(opts.startid)
        import_map = [(x, x) for x in xrange(start, int(opts.maxid))]
    else:
        import_map = []
        with open(opts.mapfile, "r") as fp:
            for line in fp.readlines():
                line = re.sub('\#.*$', '', line)
                ids = re.split('[ \t]+', line)
                for id in ids[1:]:
                    import_map.append((int(ids[0]), int(id)))
        
    for to_stream, from_stream in import_map:
        print "starting %i <- %i" % (to_stream, from_stream) 
        first = True
        vec = [(IMPORT_START,)]
        t = tic()
        data = rdb4.db_query(from_stream, IMPORT_START, IMPORT_STOP, limit=100000000, conn=db0)
        if not len(data): continue
        data = data[0]
        print "received", data.shape
        toc(t)

        t = tic()
        if opts.noop: continue
        bound = (int(data.shape[0]) / 100) + 1
        for i in xrange(0, bound):
            vec = (data[(i*100):(i*100) + 100, :]).tolist()
            # print time.ctime(vec[0][0])
            rdb4.db_add(db1, to_stream, map(tuple, vec))
            if len(vec) < 100: break
        print "inserted", to_stream
        toc(t)
        time.sleep(float(opts.delay))
        # rdb4.db_sync(db4)
    #rdb4.db_close(db0)
    rdb4.db_close(db1)
