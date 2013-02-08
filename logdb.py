from bsddb3.db import *
from log import LogRecord
from struct import pack, unpack


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


class IndexedLogDB:
    def __init__(self, baseName):
        p = DB()
        p.set_bt_compare(PKey.compareBinary)
        p.open(baseName + ".p.db", flags=DB_RDONLY)

        si = DB()
        si.open(baseName + ".sys.db", flags=DB_RDONLY)

        ti = DB()
        ti.set_bt_compare(compare_time)
        ti.open(baseName + ".time.db", flags=DB_RDONLY)

        ei = DB()
        ei.open(baseName + ".evt.db", flags=DB_RDONLY)

        p.associate(si, extract_sys  )
        p.associate(ti, extract_time )
        p.associate(ei, extract_evt  )
        
        self.primary = p
        self.system_idx = si
        self.time_idx = ti
        self.event_idx = ei

    def at_time(self, time):
        c = self.time_idx.cursor()
        k = pack('d', time)
        c.set_range(k)
        return self.Cursor(c)

    def at_sys(self,sys):
        c = self.system_idx.cursor()
        c.set_range(sys)
        return self.Cursor(sys)

    class Cursor:
        def __init__(self, dbc):
            self.dbc = dbc

        def next(self):
            r = self.dbc.next()
            if( r == None ):
                return None
            else:
                return LogRecord.from_binary(r[1])




