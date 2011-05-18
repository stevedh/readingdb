"""readingdb proxy

Instead of using IPC to grab the data from the database, forward
requests to another server running the database.
"""
import sys
import urllib2
try:
    import simplejson as json
except ImportError:
    import json

DB_URL='http://www.openbms.org/smap'


def db_open():
    return {
        'url' : DB_URL,
        'substream' : 0
        }

def db_sync(db):
    """noop"""
    return True

def db_close(db):
    """noop"""
    return True

def db_add(db):
    raise Exception("Adding to db not supported by proxy lib")

def db_substream(db, substream):
    db['substream'] = substream

def db_query(db, stream, starttime, endtime):
    """Use the real server as the source of the data"""
    url = "%s/data/%i/%i/?start=%i&end=%i" % (db['url'], stream,
                                              db['substream'],
                                              starttime, endtime)

    fp = urllib2.urlopen(url, timeout=10)
    try:
        return json.load(fp)
    finally:
        fp.close()
