
from distutils.core import setup, Extension

readingdb_module = Extension('_readingdb2',
                             sources=['readingdb2_wrap.c', 'readingdb.c', 'util.c', 'logging.c'],
                             libraries=['rt'])

setup (name='readingdb2',
       version='0.1',
       author='Stephen Dawson-Haggerty',
       ext_modules=[readingdb_module],
       py_modules='readingdb2',)
