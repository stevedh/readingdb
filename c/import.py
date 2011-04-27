
import sys
import time
import random
import MySQLdb as mq
from powerdb.smap.models import *

import readingdb

OLDSTREAM=34
NEWSTREAM=34

DBHOST='local.cs.berkeley.edu'
DBNAME='smap'
DBUSER='stevedh'
DBPASS='410soda'


db = mq.connect(passwd=DBPASS,db=DBNAME,host=DBHOST,user=DBUSER)
def imp(OLDSTREAM, NEWSTREAM):

    sql = "SELECT * FROM formatting_latest WHERE `streamid` = %i" % OLDSTREAM
    db.query(sql)
    r = db.store_result()
    row = r.fetch_row()
    if len(row) != 1:
        print "invalid formatting"
        sys.exit(1)
    formatting = row[0]

    sql = "SELECT * FROM readings WHERE `streamid` = %i" % OLDSTREAM
    db.query(sql)
    r = db.use_result()
    my_db = readingdb.db_open()

    while True:
        row = r.fetch_row(1024)
        if len(row) == 0: break
    
        for v in row:
            timestamp = v[1]
            seq = v[2]
            reading = (float(v[3]) * formatting[2]) / formatting[3]
            # print "%i %i %f" % (timestamp, seq, reading)
            readingdb.db_add(my_db, NEWSTREAM, timestamp, seq, reading)

    readingdb.db_close(my_db)

sql = "SELECT id FROM streams";
db.query(sql)
r = db.use_result()
rows = r.fetch_row(5000)
for OLDID, in rows:
    if OLDID < 2600:
        continue
    url = "http://smap.cs.berkeley.edu/db/api.py/meta?stream=%i" % OLDID
    try:
        fp = urllib2.urlopen(url)
        obj = json.load(fp)
        fp.close()
    except:
        continue
    
    pathcmp = obj['Path'].split('/')
    try:
        stream = Stream.objects.get(subscription__url=obj['SmapLocation'],
                                    channel=pathcmp[3],
                                    typ=pathcmp[2][0].upper(),
                                    point=pathcmp[1])
        print OLDID, stream.id
        imp(OLDID, stream.id)
    except:
        print "Import failed..."

# for s in Stream.objects.all():
#     print s.subscription.url + '/' + s.path()
    

db.close()

# imp(OLDSTREAM, NEWSTREAM)

sys.exit(0)

