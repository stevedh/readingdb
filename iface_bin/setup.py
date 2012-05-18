
from distutils.core import setup, Extension
import numpy as np

readingdb_module = Extension('_readingdb',
                             sources=['readingdb_wrap.c', 'readingdb.c', 'multi.c',
                                      'c6/pbuf/rdb.pb-c.c', 'c6/rpc.c'],
                             libraries=['protobuf-c'])

setup (name='readingdb-python',
       version='0.6.0',
       author='Stephen Dawson-Haggerty',
       author_email='stevedh@eecs.berkeley.edu',
       description=('Python interface to the readingdb time series database'),
       license='BSD',
       requires=['numpy'],
       url="https://github.com/stevedh/readingdb",
       ext_modules=[readingdb_module],
       include_dirs = [np.get_include()],
       py_modules=('readingdb',))
