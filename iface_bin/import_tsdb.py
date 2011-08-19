
import sys
import time
import random
import MySQLdb as mq
import threading
import Queue
import socket

from powerdb.smap.models import *
import powerdb.settings as settings


import readingdb

OLDSTREAM=34
NEWSTREAM=34

# DBHOST='jackalope.cs.berkeley.edu'
# DBNAME='powerdb'
# DBUSER='p'
# DBPASS='410soda'
assert settings.DATABASE_ENGINE == 'mysql'

class TsdbLoader(threading.Thread):
    def __init__(self, host='green.millennium.berkeley.edu', port=4242):
        threading.Thread.__init__(self)
        self.q = Queue.Queue(maxsize=10)
        self.sockaddr = (host, port)
        self.daemon = True

    def run(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(self.sockaddr)
        while True:
            add = self.q.get()
            if add['method'] == 'put':
                print "adding", len(add['data']), add['extra']
                print add['id']
                for i in xrange(0, len(add['data'])):
                    a = "put %i %i %f %s\n" % (add['id'],
                                               add['data'][i][0],
                                               add['data'][i][2],
                                               add['extra'])
                    s.send(a)
            elif add['method'] == 'quit':
                break
        s.close()

if __name__ == '__main__':
    db = mq.connect(passwd=settings.DATABASE_PASSWORD,
                    db=settings.DATABASE_NAME,
                    host=settings.DATABASE_HOST,
                    user=settings.DATABASE_USER)
    rdb = readingdb.db_open()
    loader = TsdbLoader()
    loader.start()
    
    for s in Stream.objects.all().order_by('id'):
        if s.id <= 1:
            next
        first = True
        data = []
        startts = 0
        extra = 'source=%s path=%s' % (s.subscription, s.path())
        print extra
        while first or len(data) == 10000:
            first = False
            data = readingdb.db_query(rdb, s.id, startts, 2 ** 31)

            if len(data) > 0:
                startts = data[-1][0]
                loader.q.put({'method': 'put',
                              'id': s.id,
                              'data': data,
                              'extra': extra})
                
            print startts
            # print data
        break
    loader.q.put({'method': 'quit'})
    loader.join()
    
    readingdb.db_close(rdb)
    db.close()

