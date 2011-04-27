
import time
import sys
from optparse import OptionParser

import readingdb4 as rdb4

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
                      help='how long ago to begin importing from (hours)')
    parser.add_option('-b', '--begin-streamid', dest='startid', default='0',
                      help='what streamid to start with (default=0')
    parser.add_option('-m', '--max-streamid', dest='maxid', default='10000',
                      help='what streamid to stop at (default=10000')
    parser.add_option('-d', '--delay', dest='delay', default='0.1',
                      help='how long to wait between each query-insert (default=0.1s)')
    opts, hosts = parser.parse_args()
    if len(hosts) != 2:
        parser.print_help()
        sys.exit(1)

    old_db = parse_netloc(hosts[0])
    new_db = parse_netloc(hosts[1])

    print "Importing data from %s:%i to %s:%i" % (old_db + new_db)
    print "substream: %i" % int(opts.substream)
    
    db0 = rdb4.db_open(db_host=old_db[0], db_port=old_db[1])
    db1 = rdb4.db_open(db_host=new_db[0], db_port=new_db[1])

    rdb4.db_substream(db0, int(opts.substream))
    rdb4.db_substream(db1, int(opts.substream))
    
    IMPORT_START = int(time.time()) - (int(opts.ago) * 3600)
    start = int(opts.startid)
    for stream in range(start, int(opts.maxid)):
        first = True
        vec = [(IMPORT_START,)]
        while first or len(vec) == 10000:
            first = False
            t = tic()
            vec = rdb4.db_query(db0, stream, vec[-1][0] - 1, 2**32-1)
            print "received", len(vec),
            toc(t)
            t = tic()
            rdb4.db_add(db1, stream, vec)
            print "inserted", stream, 
            toc(t)
            time.sleep(float(opts.delay))

        # rdb4.db_sync(db4)
    rdb4.db_close(db0)
    rdb4.db_close(db1)
