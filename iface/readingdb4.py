
import time
import socket

def db_open(db_host="gecko.cs.berkeley.edu", db_port=4246):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((db_host, db_port))
    except IOError:
        return None
    return s.makefile('r+')

def db_substream(db, substream):
    db.write("dbid %i\n" % substream)

def db_close(db):
    db.close()

def db_query(db, streamid, start, end, include_range=True):
    cmd = "get %i %i %i\n" % (streamid, start, end)
    db.write(cmd)
    db.flush()
    nresults = db.readline()
    nresults = int(nresults)
    rvec = [0] * nresults
    for ridx in range(0, nresults):
        val = db.readline()
        pieces = val.split(' ')
        if len(pieces) == 3 or not include_range:
            rvec[ridx] = (int(pieces[0]),
                          int(pieces[1]),
                          float(pieces[2]))
        elif len(pieces) == 5:
            rvec[ridx] = (int(pieces[0]),
                          int(pieces[1]),
                          float(pieces[2]),
                          float(pieces[3]),
                          float(pieces[4]))
        else:
            return rvec[0:ridx]
    return rvec

def db_add(db, streamid, data):
    for v in data:
        cmd = 'put %i %i %i %f' % (streamid, v[0], v[1], v[2])
        if len(v) == 5:
            cmd += ' %f %f' % (v[3], v[4])
        cmd += '\n'
        db.write(cmd)

def db_query_full(db, streamid, start, end):
    starting = True
    return_val = []
    vals = []
    mark = time.time()
    while starting or len(vals) == 10000:
        starting = False
        print db, streamid, start, end
        vals = db_query(db, streamid, start, end, include_range=False)
        return_val += vals
        start = return_val[-1][0]
        print len(vals), start
    runtime = time.time() - mark
    print "%i recs in %0.3fs" % (len(return_val), runtime)
        

    return return_val

if __name__ == '__main__':
    import time
    import sys
    db = db_open()
    db_substream(db, 1)
    #db_add(db, 1, [(int(time.time()), 1, 12)])
    #print db_query(db, 1, 0, 1000000000)
    print db_query(db, int(sys.argv[1]), int(time.time())-3600*24, int(time.time()))
    db_close(db)
