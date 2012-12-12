# -*- coding: utf-8 *-*
from bsddb3.db import DB
from log import LogRecord
from numpy import *
import matplotlib.pyplot as P

MAX_RECORDS = 1000
EPSILON = 1e-5
specified_time = 1.0

def almost_equal(a,b):
    return abs(a-b) < EPSILON

def plot_a_file(fileName):
    d = DB()
    d.open(fileName)

    p1 = []
    p2 = []

    c = d.cursor()
    for i in range(1,MAX_RECORDS):
        r = c.next()
        if(r == None):
            break;
        else:
            l = LogRecord.from_binary(r[1])
            if (almost_equal(l.time, specified_time)):
                bb = l.bodies_in_keplerian()
                p1.append(bb[1].e)
                p2.append(bb[2].e)

    P.scatter(p1,p2)
    P.show()
