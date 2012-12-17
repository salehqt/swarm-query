from bsddb3.db import DB
from log import LogRecord
from struct import pack, unpack

def extract_sys(key, data):
    (msgid, length,time,sys) = unpack('iidi',s[0:20])
    return sys

def extract_time(key, data):
    (msgid, length,time,sys) = unpack('iidi',s[0:20])
    return time

def extract_evt(key, data):
    (msgid, length,time,sys) = unpack('iidi',s[0:20])
    return msgid

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
        p.open(baseName + ".p.db")

        si = DB()
        si.open(baseName + ".sys.db")

        ti = DB()
        ti.set_bt_compare(compare_time)
        ti.open(baseName + ".time.db")

        ei = DB()
        ei.open(baseName + ".evt.db")

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




