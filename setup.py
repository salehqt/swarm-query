from distutils.core import setup
from Cython.Build import cythonize

setup(
  ext_modules = cythonize(
	  ["log.pyx","mainwindow.py","swarm_query.py", "keplerian.c"],
	  )
)
