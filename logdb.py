from bsddb3.db import *
from log import LogRecord
from struct import pack, unpack
import sys

TIME_EPSILON = sys.float_info.epsilon

class PKey:
    def __init__(self, t, s, e , r):
        self.time = t
        self.system_id = s
        self.event_id = e
        self.recno = r

    @staticmethod
    def decomposeBinary(s):
        if(len(s) == 24):
            return unpack('diiq',s)
        else:
            return None

    @staticmethod
    def fromBinary(s):
        (t,s,e,r) = PKey.decomposeBinary(s)
        return PKey(t,s,e,r)

    @staticmethod
    def compareBinary(s1,s2):
        d1 = PKey.decomposeBinary(s1)
        d2 = PKey.decomposeBinary(s2)
        if(d1 == d2):
            return 0
        elif (d1 < d2):
            return -1
        else:
            return 1

    def __repr__(self):
       m = {'time':self.time, 'sys': self.system_id, 'evt': self.event_id, 'rec#': self.recno }
       return m.__repr__();

    def toBinary(self):
        return pack('diiq', self.time, self.system_id, self.event_id, self.recno )




def extract_sys(key, data):
    return PKey.fromBinary(key).system_id 

def extract_time(key, data):
    return PKey.fromBinary(key).time

def extract_evt(key, data):
    return PKey.fromBinary(key).event_id

def compare_time(l, r):
    if(len(l) < len(r)):
        return -1
    if(len(l) > len(r)):
        return 1
    elif(len(l) != 8):
        return 0

    lf = unpack('d', l)
    rf = unpack('d', r)
    if( lf < rf ):
        return -1
    if( lf > rf ):
        return 1
    else:
        return 0


def compare_int(l, r):
    if(len(l) < len(r)):
        return -1
    if(len(l) > len(r)):
        return 1
    elif(len(l) != 4):
        return 0

    lf = unpack('i', l)
    rf = unpack('i', r)
    if( lf < rf ):
        return -1
    if( lf > rf ):
        return 1
    else:
        return 0


def iter_cursor(c, mode = DB_NEXT):
    while True:
        n = c.get(mode)
        if n != None :
            yield n
        else:
            raise StopIteration

def iter_secondary_cursor(c, mode = DB_NEXT):
    while True:
        n = c.pget(mode)
        if n != None :
            yield n
        else:
            raise StopIteration

class IndexedLogDB:
    def __init__(self, baseName):
        p = DB()
        p.set_bt_compare(PKey.compareBinary)
        p.open(baseName + ".p.db", flags=DB_RDONLY)

        si = DB()
        si.set_bt_compare(compare_int)
        si.set_dup_compare(PKey.compareBinary)
        si.open(baseName + ".sys.db", flags=DB_RDONLY)

        ti = DB()
        ti.set_bt_compare(compare_time)
        ti.set_dup_compare(PKey.compareBinary)
        ti.open(baseName + ".time.db", flags=DB_RDONLY)

        ei = DB()
        ei.set_bt_compare(compare_int)
        ei.set_dup_compare(PKey.compareBinary)
        ei.open(baseName + ".evt.db", flags=DB_RDONLY)

        p.associate(si, extract_sys  )
        p.associate(ti, extract_time )
        p.associate(ei, extract_evt  )
        
        self.primary = p
        self.system_idx = si
        self.time_idx = ti
        self.event_idx = ei


    def all_records(self):
        c = self.primary.cursor()
        for k,l in iter_cursor(c):
            yield IndexedLogDB.decodeKVP((k,l))

    def time_sequence(self, time_range):
        """Returns a list of times for a time range, it is used for making 
        range queries"""

        t0, t1 = time_range
        c = self.time_idx.cursor()
        k = pack('d', t0)
        c.set_range(k)
        c.prev();
        for k, p, l in iter_secondary_cursor(c, DB_NEXT_NODUP):
            t, = unpack('d', k)
            if t <= t1 :
                yield t
            else:
                raise StopIteration

    def system_range_records(self, sys_range):
        """Return all the records that have system id in the system range"""
        s0, s1 = sys_range
        c = self.system_idx.cursor()
        k = pack('i', s0)
        c.set_range(k)
        c.prev()
        for k, p, l in iter_secondary_cursor(c):
            s, = unpack('i', k)
            if s <= s1 :
                yield IndexedLogDB.decodeKVP((p,l))
            else:
                raise StopIteration

    def time_range_records(self, time_range):
        """Return all the records that have time in the time range"""
        t0, t1 = time_range
        c = self.time_idx.cursor()
        k = pack('d', t0)
        c.set_range(k)
        c.prev()
        for k, p, l in iter_secondary_cursor(c):
            t, = unpack('d', k)
            if t <= t1 :
                yield IndexedLogDB.decodeKVP((p,l))
            else:
                raise StopIteration



    @staticmethod
    def decodeKVP(r):
        return (PKey.fromBinary(r[0]),LogRecord.from_binary(r[1]))

    def system_range_query_at_time(self,t,system_range):
        c = self.primary.cursor()
        s0, s1 = system_range

        k = PKey(t, s0, 0, 0)
        c.set_range(k.toBinary())

        while True:
            k,l = IndexedLogDB.decodeKVP(c.current())
            if k.system_id <= s1 :
                yield (k,l)
                c.next()
            else:
                raise StopIteration






