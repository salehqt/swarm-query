from bsddb3.db import *
from log import LogRecord
from struct import pack, unpack
import sys

TIME_EPSILON = sys.float_info.epsilon

class PKey:
    def __init__(self, t, e, s):
        self.time = t
        self.event_id = e
        self.system_id = s

    @staticmethod
    def decomposeBinary(bin):
        if(len(bin) == 8):
            t, se = unpack('fI', bin)
            s = se % 2**24
            e = se / 2**24
            return (t,e,s)
        else:
            return None

    @staticmethod
    def fromBinary(bin):
        (t,e,s) = PKey.decomposeBinary(bin)
        return PKey(t,e,s)

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
       m = {'time':self.time, 'sys': self.system_id, 'evt': self.event_id  }
       return m.__repr__();

    def toBinary(self):
        return pack('fI', self.time, self.event_id * 2**24 + self.system_id)

    @staticmethod
    def packSys(s):
        return pack('I',s)
    @staticmethod
    def packTime(t):
        return pack('f',t)
    @staticmethod
    def packEvt(e):
        return pack('B',s)
    @staticmethod
    def unpackSys(bin):
        return unpack('I',bin)[0]
    @staticmethod
    def unpackEvt(bin):
        return unpack('B',bin)[0]
    @staticmethod
    def unpackTime(bin):
        return unpack('f',bin)[0]




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
    elif(len(l) != 4):
        return 0

    lf = PKey.unpackTime(l)
    rf = PKey.unpackTime(r)
    if( lf < rf ):
        return -1
    if( lf > rf ):
        return 1
    else:
        return 0


def compare_sys(l, r):
    if(len(l) < len(r)):
        return -1
    if(len(l) > len(r)):
        return 1
    elif(len(l) != 4):
        return 0

    lf = PKey.unpackSys(l)
    rf = PKey.unpackSys(r)
    if( lf < rf ):
        return -1
    if( lf > rf ):
        return 1
    else:
        return 0

def compare_evt(l, r):
    if(len(l) < len(r)):
        return -1
    if(len(l) > len(r)):
        return 1
    elif(len(l) != 1):
        return 0

    lf = PKey.unpackEvt(l)
    rf = PKey.unpackEvt(r)
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
            c.close()
            raise StopIteration

def iter_secondary_cursor(c, mode = DB_NEXT):
    while True:
        n = c.pget(mode)
        if n != None :
            yield n
        else:
            c.close()
            raise StopIteration

class IndexedLogDB:
    def __init__(self, fn):
        """Opens a BDB file that contains the primary database and
        secondary indices. fn is the path to the file"""

        o = DB()
        o.open(fn, flags=DB_RDONLY)
        for k, d in iter_cursor(o.cursor()):
            print "Database : ", k
        p = DB()
        p.set_bt_compare(PKey.compareBinary)
        p.open(fn, dbname="primary", flags=DB_RDONLY)

        si = DB()
        si.set_bt_compare(compare_sys)
        si.set_dup_compare(PKey.compareBinary)
        si.open(fn, dbname="system_idx", flags=DB_RDONLY)

        ti = DB()
        ti.set_bt_compare(compare_time)
        ti.set_dup_compare(PKey.compareBinary)
        ti.open(fn, dbname="time_idx", flags=DB_RDONLY)

        ei = DB()
        ei.set_bt_compare(compare_evt)
        ei.set_dup_compare(PKey.compareBinary)
        ei.open(fn, dbname="event_idx", flags=DB_RDONLY)

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
        k = PKey.packTime(t0)
        c.set_range(k)
        c.prev();
        for k, p, l in iter_secondary_cursor(c, DB_NEXT_NODUP):
            t = PKey.unpackTime(k)
            if t <= t1 :
                yield t
            else:
                raise StopIteration

    def system_range_records(self, sys_range):
        """Return all the records that have system id in the system range"""
        s0, s1 = sys_range
        c = self.system_idx.cursor()
        k = PKey.packSys(s0)
        c.set_range(k)
        c.prev()
        for k, p, l in iter_secondary_cursor(c):
            s = PKey.unpackSys(k)
            if s <= s1 :
                yield IndexedLogDB.decodeKVP((p,l))
            else:
                raise StopIteration

    def time_range_records(self, time_range):
        """Return all the records that have time in the time range"""
        t0, t1 = time_range
        c = self.time_idx.cursor()
        k = PKey.packTime(t0)
        c.set_range(k)
        c.prev()
        for k, p, l in iter_secondary_cursor(c):
            t = PKey.unpackTime(k)
            if t <= t1 :
                yield IndexedLogDB.decodeKVP((p,l))
            else:
                raise StopIteration

    def system_at_time(self, sysid, time_range):
        """ Apparently, DB_GET_BOTH_RANGE does not work as expected, we cannot do this"""
        t0, t1 = time_range
        c = self.system_idx.cursor()
        k = PKey.packSys(sysid)
        pk = PKey(t0, 1, sysid).toBinary()
        _, pk, _ = c.pget(k, pk, DB_GET_BOTH_RANGE)
        c.prev()
        for k, pk, l in iter_secondary_cursor(c):
            kk = PKey.fromBinary(pk)
            if t0 <= kk.time <= t1 and kk.system_id == sysid :
                yield IndexedLogDB.decodeKVP((pk,l))
            else:
                raise StopIteration

    def initial_conditions(self, system_range):
        """Return initial coniditions for the system range"""
        c = self.system_idx.cursor()

        if system_range.isUniversal() :
            s0 = 0
            s1 = sys.maxint
            c.first()
        else:
            s0, s1 = system_range.ulPair()
            k = PKey.packSys(s0)
            c.set_range(k) 

        sysid = s0
        while sysid < s1 :
            r = c.get(DB_NEXT_NODUP)
            if r : 
                ks, l = r
                sysid = PKey.unpackSys(ks)
                yield sysid, LogRecord.from_binary(l)
            else:
                break

    def final_conditions(self,system_range):
        """Return the final conditions for a range of systems that
        is the largest time for which the system has a valid entry
        right now we don't support going through all systems
        but that should not be difficult to add
        """
        c = self.system_idx.cursor()

        if system_range.isUniversal() :
            s0 = 0
            s1 = sys.maxint
            c.first()
        else:
            s0, s1 = system_range.ulPair()
            k = PKey.packSys(s0)
            c.set_range(k) 

        sysid = s0-1
        while sysid < s1  and  c.next() != None :
            if c.get(DB_NEXT_NODUP) == None :
                ks, l = c.last()
            else:
                ks, l = c.prev()
                
            sysid = PKey.unpackSys(ks)
            yield sysid, LogRecord.from_binary(l)



    @staticmethod
    def decodeKVP(r):
        return (PKey.fromBinary(r[0]),LogRecord.from_binary(r[1]))

    def system_range_for_time_event(self,time,event_id,system_range):
        c = self.primary.cursor()
        s0, s1 = system_range

        k = PKey(time, event_id, s0)
        c.set_range(k.toBinary())
        # We have to check that we are at a valid location
        c.prev()
        for r in iter_cursor(c):
            k,l = IndexedLogDB.decodeKVP(r)
            if s0 <= k.system_id <= s1 and k.time == time and k.event_id == event_id:
                yield (k,l)
            else:
                raise StopIteration


    def query(d , tr, sr, er ):

        def filterEventID(q,er):
            for k,l in q:
                if(er.contains(k.event_id)):
                    yield (k,l)    
        
        q0 = d.queryInternal(tr,sr)
        
        if(not er.isUniversal()):
            q = filterEventID(q0,er)
        else:
            q = q0

        return q

    def queryInternal(d, tr, sr):
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



