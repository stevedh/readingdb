# Create your views here.
# from django.http import HttpResponse, HttpResponseNotAllowed, HttpResponseRedirect, HttpResponseBadRequest, Http404
from powerdb.readingdb.models import *
from powerdb.readingdb.views import *

import json


if __name__ == '__main__':
    # print availability(None, 10)
    print npoints(None)
    print npoints(None, streamid=45)
