
import time
import sys
from optparse import OptionParser

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
    opts, hosts = parser.parse_args()
    if len(hosts) != 2:
        parser.print_help()
        sys.exit(1)

    old_db = parse_netloc(hosts[0])
    new_db = parse_netloc(hosts[1])

    print "Importing data from %s:%i to %s:%i" % (old_db + new_db)
    print "substream: %i" % int(opts.substream)
    
    # db0 = rdb4.db_open(host=old_db[0], port=old_db[1])
    rdb4.db_setup(old_db[0], old_db[1])
    db1 = rdb4.db_open(host=new_db[0], port=new_db[1])

    #rdb4.db_substream(db0, int(opts.substream))
    #rdb4.db_substream(db1, int(opts.substream))

    if not opts.zero:
        IMPORT_START = int(time.time()) - (int(opts.ago) * 3600)
        IMPORT_STOP = 2**32 - 10
    else:
        IMPORT_START = 1
        IMPORT_STOP = int(time.time()) - (int(opts.ago) * 3600)
    print "Importing from %i to %i" % (IMPORT_START, IMPORT_STOP)

    start = int(opts.startid)
    for stream in xrange(start, int(opts.maxid)):
        print "starting", stream
        first = True
        vec = [(IMPORT_START,)]
        t = tic()
        vec = rdb4.db_query(stream, vec[-1][0] - 1, IMPORT_STOP)
        if not len(vec): continue
        data = vec[0].tolist()
        print "received", len(data),
        toc(t)
        print data[:10]

        t = tic()
        if opts.noop: continue
        for i in xrange(0, (len(data) / 100) + 1):
            print db1, stream, data[(i*100):(i*100) + 100]
            rdb4.db_add(db1, stream, data[(i*100):(i*100) + 100])
        print "inserted", stream,
        toc(t)
        time.sleep(float(opts.delay))
        # rdb4.db_sync(db4)
        # rdb4.db_close(db0)
    rdb4.db_close(db1)
