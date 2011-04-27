
%module readingdb2
%{
#include "readingdb_py.h"
%}


IPC *db_open();
void db_substream(IPC *dbp, int substream);
void db_close(IPC *dbp);
int db_sync(IPC *dbp);
PyObject *db_query(IPC *dbp, unsigned long long streamid, 
        unsigned long long starttime, 
        unsigned long long endtime);
int db_add(IPC *dbp, int streamid, PyObject *values);
