#!/usr/bin/env python
# -*- coding: utf-8 *-*
from logdb import IndexedLogDB, PKey
from bsddb3.db import *
from log import LogRecord
from struct import pack, unpack
import argparse
from range_type import *
from functools import partial

MAX_RECORDS = 10000;



def parse_cmd():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--database", help="Database name (base name of the files that make up the database", required = True)
    parser.add_argument("-m", "--max-records", help="maximum number of records to process", default=MAX_RECORDS, type=int )
    parser.add_argument("-s", "--system-range", help="Range of systems to display",type=RangeType(int),default=Range.universal());
    parser.add_argument("-t", "--time-range", help="Range of time to display", type=RangeType(float), default=Range.universal())
    parser.add_argument("-b", "--body-range", help="Range of bodies to display", type=RangeType(int), default=Range.universal())
    parser.add_argument("-e", "--evt-id", help="The type of event to display (provide codes)",type=RangeType(int), default=Range.universal());
    parser.add_argument("-k", "--keplerian", help="Keplerian output", action="store_true")

    args = parser.parse_args()
    print args
    return args


TABLE, KEYS, KEPLERIAN = range(3)
def print_record(print_mode, r, body_range):
    k, l = r
    if print_mode == TABLE :
        i = 0
        for b in l.bodies:
            if(body_range.contains(i)):
                print "%10d %lg  %6d %6d  %9.2g  %10.4g %10.4g %10.4g  %10.5lg %10.5lg %10.5lg  %d" % (l.msgid, l.time, l.sys, i, b.mass, b.position[0], b.position[1], b.position[2], b.velocity[0], b.velocity[1], b.velocity[2], l.flags)
            i = i + 1
    elif print_mode == KEYS :
        print k


################################## MAIN ################################################

args = parse_cmd()
d = IndexedLogDB(args.database)

# default for time_range : (float('-inf'),float('+inf'))
# default for system_range : (0, sys.maxint)

#q = d.time_sys_range_query(args.time_range,args.system_range)

output_count = 0

def makeQuery(tr, sr, er):
    if(tr.isUniversal()):
       raise NotImplementedError() 
    else:
        if(sr.isUniversal()):
            raise NotImplementedError()
        else:
            for t in d.time_sequence(tr.ulPair()):
                for k,l in d.system_range_query_at_time(t,sr.ulPair()):
                    if(er.contains(k.event_id)):
                        yield (k,l)

q = makeQuery(args.time_range,args.system_range,args.evt_id)

try:
    for i in range(0,args.max_records):
        r = q.next()
        print_record(TABLE,r, args.body_range)
except StopIteration:
    pass





