# distutils: sources = keplerian.c
from bsddb3.db import *
from struct import *
cimport kepler

class LogRecord:
	class Body:
		def __init__(self,data):
			self.position, self.velocity, self.mass = data

	def to_keplerian(self):
		cdef double x,y,z,vy,vx,vz,GM, a,e,i,O,w,M
		x,y,z = self.position
		vx,vy,vz = self.velocity
		GM = self.mass
		kepler.calc_keplerian_for_cartesian(&a,&e,&i,&O,&w,&M,x,y,z,vx,vy,vz,GM)
		return (a,e,i,O,w,M)
	
	def as_map(self):
		return {'position':self.position , 'velocity':self.velocity , 'mass':self.mass}
	def __repr__(self):
		return self.as_map().__repr__()

	@staticmethod
	def from_binary(s):
		(msgid, length) = unpack('ii',s[0:8]) # msgid, len
		if msgid == 1 :
			l = LogRecord()
			(time,sys,flags,nbod) = unpack('diii',s[8:28])
			bodies = []
			for b in range(1,nbod+1):
				(x,y,z,vx,vy,vz,mass,body_id) = \
						unpack('dddddddi4x',s[(32+(b-1)*64):(32+(b)*64)])
				bodies.append(LogRecord.Body(((x,y,z),(vx,vy,vz) ,mass)))
			l.time = time
			l.sys = sys
			l.flags = flags
			l.bodies = bodies
			return l
		else:
			return None
	def as_map(self):
		return {'time':self.time, 'sys':self.sys, 'flags':self.flags, 'bodies':self.bodies }

	def __repr__(self):
		return self.as_map().__repr__();


