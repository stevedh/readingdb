
import sys
import os
import optparse
import csv

import dataloader

if __name__ == '__main__':
    parser = optparse.OptionParser(description="Fetch data from readingdb",
                                   usage="usage: %prog [options] stream specs ..")
    parser.add_option('-o', '--output', dest='output', default='-',
                      help="base name for output files")
    opts, args = parser.parse_args()

    reqvec = []
    for stream in args:
        reqvec.append({'streamid': int(stream),
                       'starttime': 0,
                       'endtime': 2**32 - 10})
    loader = dataloader.DataLoader(reqvec, host=("gecko.cs.berkeley.edu", 4243), full=True)
    data = loader.run()
    if opts.output == '-':
        print data
        pass
    else:
        for k,d in data.iteritems():
            with open(opts.output + '.' + str(k), 'w') as fp:
                writer = csv.writer(fp)
                writer.writerows(d)
