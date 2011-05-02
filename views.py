# Create your views here.
from django.http import HttpResponse
from django.db.models import Sum

from models import *

import powerdb.readingdb.iface_bin.readingdb as rdb

import json

def availability(request, streamid):
    """Return a map of stream availability -- this is a list of ranges of
the form (start, end, n) where start and end are time ranges and n is
the number of points stored in that range.  This may not be completely
accurate, in that not all points in the database may fall in this
range.
    """
    streamid = int(streamid)
    avail = StreamAvailability.objects.filter(conf__streamid__id=streamid).order_by('region_start')
    reply = HttpResponse(content_type='application/json')
    json.dump([(x.region_start, x.region_end, x.region_points) 
               for x in avail], reply)
    return reply

def npoints(request, streamid=None):
    """Look up the number of points in the database.  If streamid is not
None, it will return the number of points just for a particular
streamid.
    """
    if streamid == None:
        objs = StreamAvailability.objects.all()
    else:
        streamid = int(streamid)
        objs = StreamAvailability.objects.filter(conf__streamid__id=streamid)
    n = objs.aggregate(Sum('region_points'))
    return HttpResponse(json.dumps(n['region_points__sum']),
                                   content_type='application/json')

def iterate(request, streamid, substream, direction, reference):
    """Given the input values, find the next point (before or after) the
specified reference time.
    """
    streamid = int(streamid)
    substream = int(substream)
    reference = int(reference)

    if direction == 'prev':
        fn = rdb.db_prev
    elif direction == 'next':
        fn = rdb.db_next
    else:
        return HttpResponseBadRequest("invalid direction: must be prev or next")
    
    db = rdb.db_open()
    rdb.db_substream(db, substream)
    val = fn(db, streamid, reference)
    rdb.db_close(db)

    return HttpResponse(json.dumps(val),
                        content_type='application/json')
