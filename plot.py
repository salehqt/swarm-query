# -*- coding: utf-8 *-*
from logdb import IndexedLogDB
from log import LogRecord
from query import *
import matplotlib.pyplot as P
from range_type import *

MAX_RECORDS = 1000000
EPSILON = 1e-5
fileName = "/scratch/hpc/salehqt/hg/swarm-build/d23.db"
time_range = Range.universal()
system_range = Range.universal()
event_range = Range.single(1)



d = IndexedLogDB(fileName)
q = d.query(time_range, system_range, event_range)

p1 = []
p2 = []


for k, l in truncate(MAX_RECORDS,q):
    bb = l.bodies_in_keplerian(center=l.star())
    bodies = list(bb)

    p1.append(bodies[0][2].a)
    p2.append(bodies[0][2].e)

P.scatter(p1,p2)
P.show()


