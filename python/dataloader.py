
import sys
sys.path.append("..")

import numpy as np
import time
import threading
import Queue
import readingdb as rdb4


MAX_THREADS = 5

class DataLoader:
    """A query frontend which makes parallel requests on the readingdb server
    
    This can lead to lower latency when fetching data, in some cases.
    """
    class DataQueryThread(threading.Thread):
        def __init__(self, parent, host=None, full=False):
            threading.Thread.__init__(self)
            self.parent = parent
            self.host = host
            self.full = full
            self.daemon = True

        def run(self):
            """Dequeue requests until we're done working"""
            if self.host == None:
                db = rdb4.db_open()
            else:
                db = rdb4.db_open(host=self.host[0], port=self.host[1])
            while True:
                try:
                    request = self.parent.requests.get_nowait()
                except Queue.Empty:
                    break
                if request.has_key("substream"):
                    rdb4.db_substream(db, request['substream'])
                else:
                    rdb4.db_substream(db, 0)

                first = True
                result = []
                last = []

                while first or len(last) == 10000:
                    last = rdb4.db_query(db, int(request['streamid']),
                                         int(request['starttime']),
                                         int(request['endtime']))
                    first = False
                    print len(last)
                    result.extend(last)
                    if not self.full: break

                if self.parent.as_numpy and len(result) > 0:
                    result = np.array(result)
                    result = result[:,[0,2]]                      
                self.parent.returns[request['streamid']] = result

            rdb4.db_close(db)

    def __init__(self, requests, threads=MAX_THREADS, as_numpy=False, host=None, full=False):
        """Get a new DataLoader.

        requests: a list of streams to load.  Each should be a dict
        with "streamid", "starttime", and "endtime" keys; a
        "substream" key is optional as well.
        """

        self.requests = Queue.Queue()
        self.as_numpy = as_numpy
        self.returns = {}
        self.host = host
        self.full = full
        for r in requests:
            self.requests.put(r)
        self.n_threads = min(len(requests), threads)

    def run(self):
        """Fire off the load.  

        Returns a dict where keys are streamids, and values are the
        data returned by readingdb.
        """
        threads = []
        for i in range(0, self.n_threads):
            th = self.DataQueryThread(self, host=self.host, full=self.full)
            th.start()
            threads.append(th)
        for th in threads:
            th.join()
        return self.returns

if __name__ == '__main__':
    ids = [6100, 6126, 6048, 6074, 552, 3805]
    ids = [{'starttime' : time.time() - (3600 * 72),
            'endtime' : time.time(),
            'streamid': x} for x in ids]
    
    loader = DataLoader(ids, as_numpy=False)
    loader.run()
    
