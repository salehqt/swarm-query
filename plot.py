# -*- coding: utf-8 *-*
from logdb import IndexedLogDB
from bsddb3.db import DB
from log import LogRecord
from numpy import *
import matplotlib.pyplot as P
from struct import pack, unpack

MAX_RECORDS = 10000
EPSILON = 1e-5

def almost_equal(a,b):
    return abs(a-b) < EPSILON

def plot_a_file(fileName, time_start, time_end):
    d = IndexedLogDB(fileName)

    p1 = []
    p2 = []

    c = d.at_time(time_start)

    rec_count = 0

    for i in range(0,MAX_RECORDS):
        l = c.next()
        if(l == None):
            break;
        else:
            if(time_start < l.time < time_end):
                rec_count += 1
                bb = l.bodies_in_keplerian()
                p1.append(bb[1].a)
                p2.append(bb[2].a)
            else:
                break
    print("%i records processed" % rec_count)

    P.scatter(p1,p2)
    P.show()


from sys import argv
if(len(argv) < 4):
    print("Usage plot.py baseDbName time_start time_end")
else:
    plot_a_file(argv[1], float(argv[2]), float(argv[3]))
