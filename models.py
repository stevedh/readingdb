
import datetime
import time

from django.db import models

import powerdb.smap.models as smap
import powerdb.readingdb.iface.readingdb4 as rdb4

import numpy as np
from scipy.signal import medfilt

class StreamAvailabilityConf(models.Model):
    streamid = models.ForeignKey(smap.Stream)

    # how long to take the median over when deciding on the local rate
    median_window = models.IntegerField(default=25)

    # how many times the local rate you need to hit before declaring
    # the stream to be "down" and closing the current record
    multiple = models.IntegerField(default=10)

    last_update = models.DateTimeField(null=True)

    def update_full(self):
        db = rdb4.db_open()
        if not db: return
        StreamAvailability.objects.filter(conf=self).delete()
        try:
            data = rdb4.db_query_full(db, self.streamid.id, 0, time.time())
        finally:
            rdb4.db_close(db)

        data = np.array(data)
        deltas = data[1:,0] - data[:-1,0]

        breaks = (deltas[1:] - deltas[:-1])
        m = np.nonzero(breaks == np.max(breaks))[0]
        med = medfilt(deltas,self.median_window)

        runs = ((med * self.multiple) < deltas) * 1
        run_deltas = runs[1:] - runs[:-1]

        starts = np.nonzero(run_deltas == -1)[0]
        stops = np.nonzero(run_deltas == 1)[0]

        if stops[0] < starts[0]:
            starts = np.hstack(([0], starts))
        if starts[-1] > stops[-1]:
            stops = np.hstack((stops, len(runs)))

        print run_deltas[0:100]
        print starts.shape, stops.shape
        print starts
        print stops
        runlengths = stops - starts
        starts = starts + 1
        stops = stops + 1
        stops[-1] = len(data) - 1

        rv_breaks = np.nonzero(runlengths > self.multiple)
        breaks = np.dstack((data[starts[rv_breaks], 0], 
                            data[stops[rv_breaks], 0],
                            runlengths[rv_breaks]))[0]
        for (start, end, n, start_period, end_period) in zip(breaks[:,0],
                                                             breaks[:,1],
                                                             breaks[:,2],
                                                             med[starts[rv_breaks]-1],
                                                             med[stops[rv_breaks]-1]):
            a = StreamAvailability(conf=self,
                                   region_start=start,
                                   region_end=end,
                                   region_points=n)
            a.save()
        self.last_update = datetime.datetime.now()
        self.save()

    def __unicode__(self):
        return "StreamAvailabilityConf: streamid: %i median: %i multiple: %i" %  \
            (self.streamid.id, self.median_window, self.multiple)

class StreamAvailability(models.Model):
    conf = models.ForeignKey(StreamAvailabilityConf)
    region_start = models.IntegerField()
    region_end = models.IntegerField()
    region_points = models.IntegerField()

