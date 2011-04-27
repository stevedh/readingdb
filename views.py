# Create your views here.
from django.http import HttpResponse
from django.db.models import Sum

from models import *

import json

def availability(request, streamid):
    streamid = int(streamid)
    avail = StreamAvailability.objects.filter(conf__streamid__id=streamid).order_by('region_start')
    reply = HttpResponse(content_type='application/json')
    json.dump([(x.region_start, x.region_end, x.region_points) 
               for x in avail], reply)
    return reply

def npoints(request, streamid=None):
    if streamid == None:
        objs = StreamAvailability.objects.all()
    else:
        streamid = int(streamid)
        objs = StreamAvailability.objects.filter(conf__streamid__id=streamid)
    n = objs.aggregate(Sum('region_points'))
    return HttpResponse(json.dumps(n['region_points__sum']),
                                   content_type='application/json')

