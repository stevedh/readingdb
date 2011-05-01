
from distutils.core import setup, Extension

readingdb_module = Extension('_readingdb',
                             sources=['readingdb_wrap.c', 'readingdb.c',
                                      '../c/pbuf/rdb.pb-c.c', '../c/rpc.c'], # , 'util.c', 'logging.c'],
                             libraries=['rt', 'protobuf-c'])

setup (name='readingdb',
       version='5.0',
       author='Stephen Dawson-Haggerty',
       ext_modules=[readingdb_module],
       py_modules='readingdb',)
