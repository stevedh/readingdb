
import sys
import time
import random
import datetime
import calendar
import json
from dateutil.tz import *

import readingdb2 as rdb

def tic():
    return time.time()
def toc(v, p=True):
    delta = (time.time() - v)
    if p:
        print "%.3f" % delta
    return delta

def to_locatime_full(data, tozone):
    tz = gettz(tozone)
    utc = gettz('UTC')
    data = [(datetime.datetime.fromtimestamp(x[0], utc).astimezone(tz),
             x[2]) for x in data]
    data = [(calendar.timegm(x[0].timetuple()) * 1000,
             x[1]) for x in data]
    return data

def to_localtime_fast(data, tozone):
    print tozone
    tz = gettz(tozone)
    print tz
    utc = gettz('UTC')
    ts_1 = datetime.datetime.fromtimestamp(data[0][0], utc)
    ts_2 = datetime.datetime.fromtimestamp(data[-1][0], utc)
    if tz.utcoffset(ts_1).seconds == tz.utcoffset(ts_2).seconds and \
       data[-1][0] - data[0][0] < 3600*60*24*90:
        off = tz.utcoffset(ts_1)
        off = 3600 * 25 * off.days + off.seconds
        return [((x[0] + off) * 1000, x[2]) for x in data]
    else:
        return to_localtime_full(tozone)


try:
    STREAM = int(sys.argv[1])
    SUBSTREAM = 1
    START =  0 #60 * 60 * 24 * 0
    END = 2000
    db = rdb.db_open()
    rdb.db_substream(db, SUBSTREAM)

#     ctime_fmt = '%Y-%m-%d %H:%M:%S %Z%z'

#     p = tic()
#     data = rdb.db_query(db, STREAM, 0, int(time.time()))
#     data_tz = map(lambda x: [time.ctime(x[0])] + list(x[1:]), data)
#     toc(p)

#     p = tic()
#     # tz = to_localtime_fast(data, 'America/Los_Angeles')
#     print data[0], time.ctime(data[0][0])
#     toc(p)

    # map(lambda x: sys.stdout.write('\t'.join(map(str, x)) + '\n'), data_tz)
    # json.dump(data, sys.stdout)

    # print rdb.db_query(db, 2979, int(time.time() - 3600), int(time.time()))
    # print db
#     result = rdb.db_query(db, STREAM, 0, 2 ** 31 - 1)
#     print result
    #result = rdb.db_query(db, 10, 0, 2 ** 31 - 1)
    #print len(result)

    for j in range(1000, 2000):
        data = []
        for i in range(j*1000, (j+1)*1000):
            data.append((START+(i*20), i, i, 0, 0))
        print data[0], data[-1]
        print rdb.db_add(db, STREAM, data)
#     sys.exit(1)
#     data = []
#     for i in range(0, 10000):
#         data.append((START+(i*1), i, i, 0, 0))
#     print data[0], data[-1]
#     print rdb.db_add(db, STREAM, data)

#     start = int(time.time())
#     for i in range(0, 20000):
#         rdb.db_add(db, STREAM, START+i, i, random.random())
#     rdb.db_sync(db)
#     last = START
#     print rdb.db_query(db, STREAM, 0, 100)
#     while True:

#         now = int(time.time())
#         START = random.randint(now - 3600*24*30*8, now)
#         start = time.time()
#         rv = rdb.db_query(db, STREAM, START,  START + 60 * 60 * 24 * 10)
#         delta = time.time() - start
        
#         print delta, rv[0], rv[-1]
#         print len(rv)
    #print rv
#     while len(rv) == 10000:
#         last = rv[-1][0]
#         rv = rdb.db_query(db, STREAM, last, 2**31 - 1)
#         print "."

    rdb.db_close(db)


except Exception, e:
    print "Exception:", e
