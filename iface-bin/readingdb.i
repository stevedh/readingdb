
%module readingdb
%{
#include "readingdb_py.h"
%}


struct sock_request *db_open(char *host="jackalope.cs.berkeley.edu", short port=4243);
void db_substream(struct sock_request *dbp, int substream);
void db_close(struct sock_request *dbp);
PyObject *db_query(struct sock_request *dbp, unsigned long long streamid, 
        unsigned long long starttime, 
        unsigned long long endtime);
PyObject * db_next(struct sock_request *dbp, int streamid, unsigned long long reference);
PyObject * db_prev(struct sock_request *dbp, int streamid, unsigned long long reference);

int db_add(struct sock_request *dbp, int streamid, PyObject *data);
