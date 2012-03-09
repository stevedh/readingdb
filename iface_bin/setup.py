
from distutils.core import setup, Extension

readingdb_module = Extension('_readingdb',
                             sources=['readingdb_wrap.c', 'readingdb.c',
                                      '../c6/pbuf/rdb.pb-c.c', '../c6/rpc.c'],
                             libraries=['protobuf-c'])

setup (name='readingdb',
       version='0.7',
       author='Stephen Dawson-Haggerty',
       author_email='stevedh@eecs.berkeley.edu',
       description=('Python interface to the readingdb time series database'),
       license='BSD',
       ext_modules=[readingdb_module],
       py_modules=('readingdb',))
