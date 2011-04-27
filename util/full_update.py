
import sys

from powerdb.readingdb.models import *
import powerdb.smap.models as smap

if __name__ == '__main__':
    if len(sys.argv) == 2:
        startidx = int(sys.argv[1])
    else:
        startidx = 0
    for str in smap.Stream.objects.filter(id__gt=startidx):
        conf, created = StreamAvailabilityConf.objects.get_or_create(streamid=str)
        print conf
        conf.update_full()

