#!/usr/bin/env python
# -*- coding: utf-8 *-*
from logdb import IndexedLogDB, PKey
from bsddb3.db import *
from log import LogRecord
from struct import pack, unpack

MAX_RECORDS = 10000;


import argparse

class RangeType(object):
    def __init__(self,type=int):
        self.type = type
    def __call__(self, string):
        x = string.split("..")
        if (len(x) == 2):
            return (self.type(x[0]),self.type(x[1]))
#        elif( len(x) == 1):
#            return self.type(x[0])
        else:
            raise argparse.ArgumentTypeError("Invalid range '%s' valid range should be like '1..2' " % string) 


parser = argparse.ArgumentParser()
parser.add_argument("-d", "--database", help="Database name (base name of the files that make up the database", required = True)
parser.add_argument("-m", "--max-records", help="maximum number of records to process", default=MAX_RECORDS, type=int )
parser.add_argument("-s", "--system-range", help="Range of systems to display",type=RangeType(int));
parser.add_argument("-t", "--time-range", help="Range of time to display", type=RangeType(float))
parser.add_argument("-e", "--evt-id", help="The type of event to display");
parser.add_argument("-k", "--keplerian", help="Keplerian output", action="store_true")

args = parser.parse_args()
print args

## Do the query ####
#    dump_a_file(argv[1], int(argv[2]), int(argv[3]))

from functools import partial

TABLE, KEYS, KEPLERIAN = range(3)
def print_record(print_mode, r):
    k, l = r
    if print_mode == TABLE :
        i = 0
        for b in l.bodies:
            print "%10d %lg  %6d %6d  %9.2g  %10.4g %10.4g %10.4g  %10.5lg %10.5lg %10.5lg  %d" % (l.msgid, l.time, l.sys, i, b.mass, b.position[0], b.position[1], b.position[2], b.velocity[0], b.velocity[1], b.velocity[2], l.flags)
            i = i + 1
    elif print_mode == KEYS :
        print k




d = IndexedLogDB(args.database)

# default for time_range : (float('-inf'),float('+inf'))
# default for system_range : (0, sys.maxint)

#q = d.time_sys_range_query(args.time_range,args.system_range)

for t in d.time_sequence(args.time_range):
    print "Time: ", t
    for k,l in d.system_range_query_at_time(t,args.system_range):
        print_record(TABLE, (k,l))
    print "\n"


