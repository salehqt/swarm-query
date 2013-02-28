#!/usr/bin/env python
# -*- coding: utf-8 *-*
from logdb import IndexedLogDB, PKey
from bsddb3.db import *
from log import LogRecord
from struct import pack, unpack
import argparse
from range_type import *
from functools import partial
import math
from exceptions import *

MAX_RECORDS = 10000;


RAD2DEG = 180.0/math.pi
class Keplerian(object):
    ASTROCENTRIC, BARYCENTRIC, JACOBI, ORIGIN = range(4)
    choices = [ "astrocentric", "barycentric", "jacobi", "origin" ]
    def __init__(self,type):
        if isinstance(type,str):
            self.type = Keplerian.choices.index(type)
        else:
            self.type = type
    def __repr__(self):
        return "Keplerian(%s)" % Keplerian.choices[self.type]

def parse_cmd():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--database", help="Database filename", required = True)
    parser.add_argument("-m", "--max-records", help="maximum number of records to process", default=MAX_RECORDS, type=int )
    parser.add_argument("-s", "--system-range", help="Range of systems to display",type=RangeType(int),default=Range.universal());
    parser.add_argument("-t", "--time-range", help="Range of time to display", type=RangeType(float), default=Range.universal())
    parser.add_argument("-b", "--body-range", help="Range of bodies to display", type=RangeType(int), default=Range.universal())
    parser.add_argument("-e", "--evt-id", help="The type of event to display (provide codes)",type=RangeType(int), default=Range.universal());
    parser.add_argument("-k", "--keplerian", help="Keplerian output. The argument defines the type of coordinate system used", choices = Keplerian.choices)

    args = parser.parse_args()
    print args
    return args



TABLE, KEYS = range(2)
def print_record(print_mode, r, body_range):
    k, l = r
    if print_mode == TABLE :
        i = 0
        for b in l.bodies:
            if(body_range.contains(i)):
                print "%10d %lg  %6d %6d  %9.2g  %10.4g %10.4g %10.4g  %10.5lg %10.5lg %10.5lg  %d" % (l.msgid, l.time, l.sys, i, b.mass, b.position[0], b.position[1], b.position[2], b.velocity[0], b.velocity[1], b.velocity[2], l.flags)
            i = i + 1

    elif isinstance( print_mode , Keplerian ):
        tp = print_mode.type
        if tp == Keplerian.JACOBI:
            body_orbits = l.bodies_in_keplerian_jacobi()
        else:
            if tp == Keplerian.ASTROCENTRIC:
                center = l.star()
            elif tp == Keplerian.BARYCENTRIC:
                center = l.barycenter()
            elif tp == Keplerian.ORIGIN:
                center = l.origin()
            else:
                raise ValueError("Wrong keplerian index")
            body_orbits = l.bodies_in_keplerian(center)
        
        for i, b, orbit in body_orbits:
            if(body_range.contains(i)):
                print "%10d %lg  %6d %6d  %9.2g  %9.5lg %9.5lg %9.5lg  %9.5lg %9.5lg %9.5lg  %d" % (l.msgid, l.time, l.sys, i, b.mass, orbit.a, orbit.e , orbit.i*RAD2DEG, orbit.O*RAD2DEG, orbit.w *RAD2DEG, orbit.M*RAD2DEG, l.flags)

    elif print_mode == KEYS :
        print k
    else:
        print print_mode, type(print_mode)


################################## MAIN ################################################

args = parse_cmd()
d = IndexedLogDB(args.database)

def makeQuery(tr, sr):
    if(tr.isUniversal()):
        if(sr.isUniversal()):
            for k,l in d.all_records():
                yield (k,l)
        else:
            for k,l in d.system_range_records(sr.ulPair()):
                yield (k,l)
    else:
        if(sr.isUniversal()):
            for k,l in d.time_range_records(tr.ulPair()):
                yield (k,l)
        else:
            for t in d.time_sequence(tr.ulPair()):
                for k,l in d.system_range_for_time_event(t,1,sr.ulPair()):
                    yield (k,l)

def filterEventID(q,er):
    for k,l in q:
        if(er.contains(k.event_id)):
            yield (k,l)

q0 = makeQuery(args.time_range,args.system_range)

if(not args.evt_id.isUniversal()):
    q = filterEventID(q0,args.evt_id)
else:
    q = q0

if args.keplerian != None:
    print_mode  = Keplerian(args.keplerian)
else:
    print_mode  = TABLE

try:
    for i in range(0,args.max_records):
        r = q.next()
        print_record(print_mode,r, args.body_range)
except StopIteration:
    pass





